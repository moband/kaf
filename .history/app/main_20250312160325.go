// Package main is the entry point for the Kafka server
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/kafka-starter-go/internal/server"
	"github.com/codecrafters-io/kafka-starter-go/pkg/logger"
)

func main() {
	log := logger.New(logger.INFO)
	log.Info("Kafka server starting...")

	config := server.Config{
		Host: "0.0.0.0",
		Port: 9092,
	}

	// Create and start the server
	srv := server.New(config, log)
	if err := srv.Start(); err != nil {
		log.Error("Failed to start server: %s", err.Error())
		os.Exit(1)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for interrupt signal
	<-sigChan
	log.Info("Shutting down server...")

	// Stop the server
	if err := srv.Stop(); err != nil {
		log.Error("Error during shutdown: %s", err.Error())
		os.Exit(1)
	}
}
