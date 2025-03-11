package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/handler"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var correlationId, length int32
		var requestApiKey, requestApiVersion int16

		// messageSize := 0
		binary.Read(conn, binary.BigEndian, &length)
		binary.Read(conn, binary.BigEndian, &requestApiKey)
		binary.Read(conn, binary.BigEndian, &requestApiVersion)
		binary.Read(conn, binary.BigEndian, &correlationId)
		fmt.Println(length)
		fmt.Println(requestApiKey)
		fmt.Println(requestApiVersion)
		fmt.Println(correlationId)

		tmp := make([]byte, length-8)
		binary.Read(conn, binary.BigEndian, &tmp)
		// // messageSize := buffer[0:4]
		// requestApiKey := buffer[4:6]
		// requestApiVersion := buffer[6:8]
		// correlationId := binary.BigEndian.Uint32(buffer[8:12])
		// // tagBuffer := buffer[12:]
		// intRequestApiVersion := int16(binary.BigEndian.Uint16(requestApiVersion))

		if requestApiVersion < 0 || requestApiVersion > 4 {
			out := make([]byte, 6)
			binary.BigEndian.PutUint32(out, uint32(correlationId))
			binary.BigEndian.PutUint16(out[4:], uint16(35))
			send(conn, out)
			os.Exit(1)
		}
		// errorSlice := make([]byte, 2)

		// // Convert int16 to []byte (big-endian)
		// binary.BigEndian.PutUint16(errorSlice, uint16(error))

		out := make([]byte, 26)
		binary.BigEndian.PutUint32(out, uint32(correlationId))
		binary.BigEndian.PutUint16(out[4:], 0)   // Error code
		out[6] = 3                               // Number of API keys
		binary.BigEndian.PutUint16(out[7:], 18)  // API Key 1 - API_VERSIONS
		binary.BigEndian.PutUint16(out[9:], 3)   //             min version
		binary.BigEndian.PutUint16(out[11:], 4)  //             max version
		out[13] = 0                              // _tagged_fields
		binary.BigEndian.PutUint16(out[14:], 75) //
		binary.BigEndian.PutUint16(out[16:], 0)  //             min version
		binary.BigEndian.PutUint16(out[18:], 0)  //             max version
		out[20] = 0                              // _tagged_fields
		binary.BigEndian.PutUint32(out[21:], 0)  // throttle time
		out[25] = 0

		send(conn, out)
	}
}

func send(conn net.Conn, out []byte) {
	binary.Write(conn, binary.BigEndian, int32(len(out)))
	binary.Write(conn, binary.BigEndian, out)
}
