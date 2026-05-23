package handlers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/AegisStream/fraud-ledger-engine/ingestion-api/config"
	"github.com/gofiber/fiber/v2"
)

// ChargeRequest defines the strict incoming schema for a transaction ingestion payload.
type ChargeRequest struct {
	TxnID    string  `json:"txn_id"`
	UserID   string  `json:"user_id"`
	Amount   float64 `json:"amount"`
	DeviceID string  `json:"device_id"`
}

// ChargeResponse defines the response schema returned on successful ingestion.
type ChargeResponse struct {
	Status    string    `json:"status"`
	TxnID     string    `json:"txn_id"`
	Timestamp time.Time `json:"timestamp"`
}

// ErrorResponse defines a standard error format for client communication.
type ErrorResponse struct {
	Error     string    `json:"error"`
	Details   string    `json:"details,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// HandlePayment processes the payment ingestion HTTP request.
func HandlePayment(prod *config.KafkaProducer) fiber.Handler {
	return func(c *fiber.Ctx) error {
		var req ChargeRequest

		// 1. Parse JSON Payload
		if err := c.BodyParser(&req); err != nil {
			log.Printf("[WARN] Failed to parse request body: %v", err)
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error:     "Invalid Request JSON",
				Details:   err.Error(),
				Timestamp: time.Now(),
			})
		}

		// 2. Perform Strict Validation
		if req.TxnID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error:     "Validation Error",
				Details:   "txn_id is a required field",
				Timestamp: time.Now(),
			})
		}
		if req.UserID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error:     "Validation Error",
				Details:   "user_id is a required field",
				Timestamp: time.Now(),
			})
		}
		if req.Amount <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error:     "Validation Error",
				Details:   "amount must be strictly positive",
				Timestamp: time.Now(),
			})
		}
		if req.DeviceID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{
				Error:     "Validation Error",
				Details:   "device_id is a required field",
				Timestamp: time.Now(),
			})
		}

		// 3. Serialize structured event for downstream processing
		payloadBytes, err := json.Marshal(req)
		if err != nil {
			log.Printf("[ERROR] Failed to marshal transaction payload: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(ErrorResponse{
				Error:     "Internal Server Error",
				Details:   "serialization failure",
				Timestamp: time.Now(),
			})
		}

		// 4. Publish Event to raw-transactions Kafka topic with context timeout
		ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
		defer cancel()

		// UserID is used as the partition key to guarantee sequencing of events per user!
		err = prod.PublishMessage(ctx, req.UserID, payloadBytes)
		if err != nil {
			log.Printf("[ERROR] Kafka publish failed for txn %s: %v", req.TxnID, err)
			return c.Status(fiber.StatusInternalServerError).JSON(ErrorResponse{
				Error:     "Ingestion Failed",
				Details:   "Failed to write event to broker",
				Timestamp: time.Now(),
			})
		}

		// 5. Respond 202 Accepted
		return c.Status(fiber.StatusAccepted).JSON(ChargeResponse{
			Status:    "queued",
			TxnID:     req.TxnID,
			Timestamp: time.Now(),
		})
	}
}
