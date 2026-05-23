package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AegisStream/fraud-ledger-engine/ingestion-api/config"
	"github.com/AegisStream/fraud-ledger-engine/ingestion-api/handlers"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

func main() {
	log.Println("[INFO] Starting Real-Time Ingestion API...")

	// 1. Load Configurations from Environment Variables
	port := getEnv("PORT", ":8081")
	brokerString := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "raw-transactions")

	brokers := strings.Split(brokerString, ",")

	// 2. Initialize Kafka Producer
	producer := config.NewKafkaProducer(brokers, topic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("[ERROR] Error closing Kafka producer on exit: %v", err)
		}
	}()

	// 3. Initialize Fiber HTTP Engine
	app := fiber.New(fiber.Config{
		DisableStartupMessage: false,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          10 * time.Second,
	})

	// 4. Register Standard Middlewares
	app.Use(recover.New()) // Capture panics and return 500 automatically
	app.Use(logger.New(logger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path}\n",
	}))

	// 5. Register Routes
	app.Post("/charge", handlers.HandlePayment(producer))

	// Health check endpoint for container probes
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status":    "UP",
			"timestamp": time.Now(),
		})
	})

	// 6. Start HTTP Server in Goroutine to facilitate graceful shutdown
	go func() {
		log.Printf("[INFO] Ingestion API listening on %s", port)
		if err := app.Listen(port); err != nil {
			log.Printf("[INFO] Server stopped: %v", err)
		}
	}()

	// 7. Setup Graceful Shutdown Listener
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("[INFO] Graceful shutdown initiated. Terminating server...")

	// Create a context with timeout for closing active connections
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP Server
	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Printf("[ERROR] Fiber server shutdown error: %v", err)
	} else {
		log.Println("[INFO] Fiber server stopped accepting new requests")
	}

	log.Println("[INFO] Server shutdown complete. Goodbye!")
}

// getEnv fetches an environment variable or returns a fallback value.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
