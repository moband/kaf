package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/handler"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

func main() {
	// Initialize logger
	logLevel := utils.LogLevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		logLevel = utils.GetLevelFromString(levelStr)
	}
	logger := utils.NewLogger(logLevel)

	// Log startup message
	logger.Infof("Starting Kafka server")

	// Load configuration
	cfg := config.NewConfig()
	logger.Infof("Server configured to listen on %s", cfg.GetListenAddress())

	// Create connection handler
	connectionHandler := handler.NewConnectionHandler(cfg, logger)

	// Start server
	logger.Infof("Starting TCP server on %s", cfg.GetListenAddress())
	listener, err := startServer(cfg.GetListenAddress())
	if err != nil {
		logger.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	// Accept connections
	acceptConnections(listener, connectionHandler, logger)
}

// startServer starts the TCP server
func startServer(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to %s: %v", address, err)
	}
	return listener, nil
}

// acceptConnections accepts and handles incoming connections
func acceptConnections(listener net.Listener, connectionHandler *handler.ConnectionHandler, logger *utils.Logger) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				logger.Infof("Server shutting down")
				return
			}
			logger.Errorf("Error accepting connection: %v", err)
			continue
		}

		logger.Debugf("Accepted connection from %s", conn.RemoteAddr())
		go connectionHandler.HandleConnection(conn)
	}
}
