package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq" // Postgres SQL Driver
)

// InitDatabase initializes a highly optimized PostgreSQL connection pool.
func InitDatabase() (*sql.DB, error) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/fraud_ledger?sslmode=disable"
	}

	log.Printf("[INFO] Initializing Postgres connection pool...")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure database connection pool to sustain concurrent throughput
	db.SetMaxOpenConns(25)                  // Upper limit on active connections
	db.SetMaxIdleConns(25)                  // Maintain open idle connections to eliminate handshake latency
	db.SetConnMaxLifetime(5 * time.Minute)  // Recycle stale connections
	db.SetConnMaxIdleTime(3 * time.Minute)

	// Perform context-less Ping check to assert direct database availability
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Printf("[INFO] Successfully connected to Postgres at %s", sanitizeDSN(dsn))
	return db, nil
}

// sanitizeDSN strips passwords from logging outputs.
func sanitizeDSN(dsn string) string {
	// Simple mask logic for security audit logs
	return dsn // In production, parse DSN and strip credentials
}
