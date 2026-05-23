package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AegisStream/fraud-ledger-engine/ledger-worker/database"
	"github.com/segmentio/kafka-go"
)

// ApprovedTransaction represents the data payload consumed from approved-transactions topic.
type ApprovedTransaction struct {
	TxnID      string  `json:"txn_id"`
	UserID     string  `json:"user_id"`
	Amount     float64 `json:"amount"`
	DeviceID   string  `json:"device_id"`
	FraudScore float64 `json:"fraud_score"`
}

func main() {
	log.Println("[INFO] Starting Ledger Worker...")

	// 1. Load Configurations from Environment Variables
	brokerString := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "approved-transactions")
	groupID := getEnv("CONSUMER_GROUP", "ledger-worker-group")

	brokers := strings.Split(brokerString, ",")

	// 2. Initialize PostgreSQL Connection Pool
	db, err := database.InitDatabase()
	if err != nil {
		log.Fatalf("[CRITICAL] Database initialization failed: %v", err)
	}
	defer db.Close()

	// 3. Initialize Kafka Consumer Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          brokers,
		GroupID:          groupID,
		Topic:            topic,
		MinBytes:         10,   // 10B
		MaxBytes:         10e6, // 10MB
		MaxWait:          1 * time.Second,
		CommitInterval:   0, // Critical: Disable auto-commit to execute manual commits after SQL transactions!
		StartOffset:      kafka.FirstOffset,
		ReadBackoffMin:   100 * time.Millisecond,
		ReadBackoffMax:   1 * time.Second,
	})
	defer func() {
		log.Println("[INFO] Shutting down Kafka Reader...")
		if err := reader.Close(); err != nil {
			log.Printf("[ERROR] Error closing Kafka reader: %v", err)
		}
	}()

	log.Printf("[INFO] Subscribed to topic '%s' using group ID '%s'", topic, groupID)

	// Setup context with cancellation for graceful shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Setup OS Signal Interception
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("[INFO] Termination signal captured. Cancelling consumer contexts...")
		cancel()
	}()

	// 5. Run Consumer Loop
	for {
		// Poll Kafka for messages (respecting graceful shutdown context)
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("[INFO] Fetch loop broken by context cancellation")
				break
			}
			log.Printf("[ERROR] Failed to fetch message from Kafka: %v", err)
			continue
		}

		// 6. Process message inside an isolated function boundary to manage defers/errors cleanly
		startTime := time.Now()
		err = processApprovedTransaction(ctx, db, msg.Value)
		if err != nil {
			log.Printf("[ERROR] Transaction processing failed for key %s: %v", string(msg.Key), err)
			// Depending on policy, we can retry, push to Dead-Letter Queue (DLQ), or halt.
			// For this implementation, we log the failure and skip to maintain ledger availability.
			continue
		}

		// 7. Manually commit the Kafka offset only AFTER successful ACID db commit!
		err = reader.CommitMessages(ctx, msg)
		if err != nil {
			log.Printf("[ERROR] Failed to commit Kafka offset: %v", err)
			continue
		}

		log.Printf("[INFO] Transaction verified, journal entries written, offset committed in %v", time.Since(startTime))
	}

	log.Println("[INFO] Ledger Worker stopped successfully. Goodbye!")
}

// processApprovedTransaction runs the ACID-compliant double-entry ledger updates.
func processApprovedTransaction(ctx context.Context, db *sql.DB, messageVal []byte) error {
	var txData ApprovedTransaction
	if err := json.Unmarshal(messageVal, &txData); err != nil {
		return fmtErrorf("failed to parse JSON payload: %w", err)
	}

	// Begin SQL Transaction with strict Read Committed isolation
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmtErrorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Safe: No-op if transaction committed successfully

	// 1. ENFORCE STRICT IDEMPOTENCY
	// Attempt to reserve the TxnID. If it exists, ON CONFLICT does nothing.
	res, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (idempotency_key, status, response_code, response_body)
		VALUES ($1, 'PROCESSING', 202, 'QUEUED')
		ON CONFLICT (idempotency_key) DO NOTHING;
	`, txData.TxnID)
	if err != nil {
		return fmtErrorf("failed to insert idempotency key: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmtErrorf("failed to fetch rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Duplicate TxnID detected! Let's assert its state.
		var status string
		err = tx.QueryRowContext(ctx, `
			SELECT status FROM idempotency_keys WHERE idempotency_key = $1;
		`, txData.TxnID).Scan(&status)
		if err != nil {
			return fmtErrorf("failed to check duplicate key status: %w", err)
		}

		log.Printf("[WARN] Duplicate transaction ID detected: %s | State: %s. Skipping duplicates.", txData.TxnID, status)
		return nil // Success fallback (idempotent ignore)
	}

	// 2. PREVENT CONCURRENT DB DEADLOCKS VIA DETERMINISTIC LOCK ORDERING
	// Payer Account = txData.UserID, Payee Account = "merchant_treasury"
	payerID := txData.UserID
	treasuryID := "merchant_treasury"

	var firstLock, secondLock string
	if payerID < treasuryID {
		firstLock = payerID
		secondLock = treasuryID
	} else {
		firstLock = treasuryID
		secondLock = payerID
	}

	// Perform explicit Pessimistic Row Locking in the deterministic order
	var firstBalance, secondBalance float64
	err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1 FOR UPDATE;", firstLock).Scan(&firstBalance)
	if err != nil {
		return fmtErrorf("failed to lock first account %s: %w", firstLock, err)
	}
	err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1 FOR UPDATE;", secondLock).Scan(&secondBalance)
	if err != nil {
		return fmtErrorf("failed to lock second account %s: %w", secondLock, err)
	}

	// Re-map variables to payer vs. treasury
	var payerBalance, treasuryBalance float64
	if payerID == firstLock {
		payerBalance = firstBalance
		treasuryBalance = secondBalance
	} else {
		payerBalance = secondBalance
		treasuryBalance = firstBalance
	}

	// 3. BALANCE CHECKS (ACID constraint check in app logic before updates)
	if payerBalance < txData.Amount {
		log.Printf("[WARN] Insufficient balance for user %s. Payer Balance: $%.2f, Charge Amount: $%.2f", payerID, payerBalance, txData.Amount)

		// Record the failed transaction attempt
		_, err = tx.ExecContext(ctx, `
			INSERT INTO transactions (id, user_id, amount, device_id, status, fraud_score, created_at)
			VALUES ($1, $2, $3, $4, 'DECLINED_INSUFFICIENT_FUNDS', $5, CURRENT_TIMESTAMP);
		`, txData.TxnID, payerID, txData.Amount, txData.DeviceID, txData.FraudScore)
		if err != nil {
			return fmtErrorf("failed to write failed transaction audit: %w", err)
		}

		// Update Idempotency Key state
		_, err = tx.ExecContext(ctx, `
			UPDATE idempotency_keys
			SET status = 'FAILED', response_code = 400, response_body = 'DECLINED_INSUFFICIENT_FUNDS', updated_at = CURRENT_TIMESTAMP
			WHERE idempotency_key = $1;
		`, txData.TxnID)
		if err != nil {
			return fmtErrorf("failed to update failed idempotency key: %w", err)
		}

		// Commit the transaction to store the DECLINED audit trail!
		if err := tx.Commit(); err != nil {
			return fmtErrorf("failed to commit declined transaction: %w", err)
		}
		return nil
	}

	// 4. EXECUTE TRANSFER
	payerBalanceAfter := payerBalance - txData.Amount
	treasuryBalanceAfter := treasuryBalance + txData.Amount

	// Round balances to 4 decimal precision to prevent float leakage
	payerBalanceAfter = math.Round(payerBalanceAfter*10000) / 10000
	treasuryBalanceAfter = math.Round(treasuryBalanceAfter*10000) / 10000

	// Deduct from payer
	_, err = tx.ExecContext(ctx, `
		UPDATE accounts SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;
	`, payerBalanceAfter, payerID)
	if err != nil {
		return fmtErrorf("failed to deduct payer balance: %w", err)
	}

	// Credit to treasury
	_, err = tx.ExecContext(ctx, `
		UPDATE accounts SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2;
	`, treasuryBalanceAfter, treasuryID)
	if err != nil {
		return fmtErrorf("failed to credit merchant treasury: %w", err)
	}

	// 5. WRITE TRANSACTION AUDIT AND IMMUTABLE LEDGER ENTRIES
	// Insert transaction log
	_, err = tx.ExecContext(ctx, `
		INSERT INTO transactions (id, user_id, amount, device_id, status, fraud_score, created_at)
		VALUES ($1, $2, $3, $4, 'APPROVED', $5, CURRENT_TIMESTAMP);
	`, txData.TxnID, payerID, txData.Amount, txData.DeviceID, txData.FraudScore)
	if err != nil {
		return fmtErrorf("failed to write approved transaction audit: %w", err)
	}

	// Double-Entry Journal Rows
	// Entry A: Payer DEBIT (balance decreases)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, direction, amount, balance_after, created_at)
		VALUES ($1, $2, 'DEBIT', $3, $4, CURRENT_TIMESTAMP);
	`, txData.TxnID, payerID, txData.Amount, payerBalanceAfter)
	if err != nil {
		return fmtErrorf("failed to record payer ledger entry: %w", err)
	}

	// Entry B: Treasury CREDIT (balance increases)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, direction, amount, balance_after, created_at)
		VALUES ($1, $2, 'CREDIT', $3, $4, CURRENT_TIMESTAMP);
	`, txData.TxnID, treasuryID, txData.Amount, treasuryBalanceAfter)
	if err != nil {
		return fmtErrorf("failed to record treasury ledger entry: %w", err)
	}

	// 6. FINALIZE IDEMPOTENCY KEY TO COMPLETED
	_, err = tx.ExecContext(ctx, `
		UPDATE idempotency_keys
		SET status = 'COMPLETED', response_code = 200, response_body = 'APPROVED', updated_at = CURRENT_TIMESTAMP
		WHERE idempotency_key = $1;
	`, txData.TxnID)
	if err != nil {
		return fmtErrorf("failed to finalize idempotency key: %w", err)
	}

	// Commit ACID Transaction
	if err := tx.Commit(); err != nil {
		return fmtErrorf("failed to commit transaction: %w", err)
	}

	log.Printf("[SUCCESS] Charged user %s $%.2f | Fraud Score: %.4f | Balance: $%.2f | TxnID: %s", 
		payerID, txData.Amount, txData.FraudScore, payerBalanceAfter, txData.TxnID)
	return nil
}

// fmtErrorf wraps errors for standard logging.
func fmtErrorf(format string, args ...any) error {
	import "fmt"
	return fmt.Errorf(format, args...)
}

// getEnv fetches an environment variable or returns a fallback value.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
