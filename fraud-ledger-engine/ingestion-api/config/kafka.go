package config

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer manages the connection lifecycle and publishing to Kafka.
type KafkaProducer struct {
	Writer *kafka.Writer
}

// NewKafkaProducer configures and initializes a high-performance Kafka writer.
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Crucial: Routes same UserID to the same partition to guarantee processing order!
		MaxAttempts:  5,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll, // Enforce ACID write consensus across replicas (acks = -1)
		Async:        false,            // Synchronous delivery check before returning 202 to client, or true for max speed.
		                               // Let's do synchronous with a timeout for production safety and reliability.
	}

	log.Printf("[INFO] Initialized Kafka Producer to brokers %v on topic '%s'", brokers, topic)
	return &KafkaProducer{Writer: writer}
}

// PublishMessage sends a payload to Kafka with key-based partitioning.
func (kp *KafkaProducer) PublishMessage(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	startTime := time.Now()
	err := kp.Writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	log.Printf("[DEBUG] Successfully published message to Kafka (Key: %s, Size: %d bytes) in %v", 
		key, len(value), time.Since(startTime))
	return nil
}

// Close gracefully flushes and closes the Kafka writer connection.
func (kp *KafkaProducer) Close() error {
	log.Println("[INFO] Shutting down Kafka Producer...")
	if err := kp.Writer.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka writer: %w", err)
	}
	log.Println("[INFO] Kafka Producer closed successfully")
	return nil
}
