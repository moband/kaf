package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

// Kafka protocol constants
const (
	// API Keys
	ApiVersionsKey uint16 = 18

	// Error codes
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35

	// API Version support
	MinSupportedVersion uint16 = 0
	MaxSupportedVersion uint16 = 4
)

// handleConnection processes a single Kafka client connection
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the message size (4 bytes)
	var length int32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		log.Printf("Error reading message size: %v", err)
		return
	}

	// Read the API key (2 bytes)
	var requestApiKey int16
	if err := binary.Read(conn, binary.BigEndian, &requestApiKey); err != nil {
		log.Printf("Error reading API key: %v", err)
		return
	}

	// Read the API version (2 bytes)
	var requestApiVersion int16
	if err := binary.Read(conn, binary.BigEndian, &requestApiVersion); err != nil {
		log.Printf("Error reading API version: %v", err)
		return
	}

	// Read the correlation ID (4 bytes)
	var correlationId int32
	if err := binary.Read(conn, binary.BigEndian, &correlationId); err != nil {
		log.Printf("Error reading correlation ID: %v", err)
		return
	}

	// Read the rest of the message
	rest := make([]byte, length-8)
	if _, err := io.ReadFull(conn, rest); err != nil {
		log.Printf("Error reading rest of message: %v", err)
		return
	}

	// Determine if the API version is supported
	errorCode := ErrorNone
	if requestApiVersion < 0 || requestApiVersion > MaxSupportedVersion {
		errorCode = ErrorUnsupportedVersion
	}

	// Send response
	if err := sendApiVersionsResponse(conn, correlationId, errorCode); err != nil {
		log.Printf("Error sending response: %v", err)
	}
}

// sendApiVersionsResponse sends a Kafka ApiVersions response
func sendApiVersionsResponse(conn net.Conn, correlationId int32, errorCode uint16) error {
	// Create response buffer without the message size (will be prepended by send())
	responseBody := make([]byte, 15)

	// Fill in correlation ID
	binary.BigEndian.PutUint32(responseBody[0:4], uint32(correlationId))

	// Fill in error code (2 bytes)
	binary.BigEndian.PutUint16(responseBody[4:6], errorCode)

	// Fill in throttle time (4 bytes) - 0ms
	binary.BigEndian.PutUint32(responseBody[6:10], 0)

	// API Versions array
	binary.BigEndian.PutUint16(responseBody[10:12], 1) // Array length (1 entry)

	// API entry
	binary.BigEndian.PutUint16(responseBody[12:14], ApiVersionsKey)      // API key (18)
	binary.BigEndian.PutUint16(responseBody[14:16], MinSupportedVersion) // Min version (0)
	binary.BigEndian.PutUint16(responseBody[16:18], MaxSupportedVersion) // Max version (4)

	// Tagged fields (0)
	responseBody[18] = 0

	// Send the response
	return send(conn, responseBody)
}

// send writes the message size followed by the message content
func send(conn net.Conn, data []byte) error {
	// Write message size (4 bytes)
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return fmt.Errorf("failed to write message size: %w", err)
	}

	// Write message content
	if err := binary.Write(conn, binary.BigEndian, data); err != nil {
		return fmt.Errorf("failed to write message content: %w", err)
	}

	return nil
}

func main() {
	// Listen for connections
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to bind to port 9092: %v", err)
	}
	defer listener.Close()

	log.Println("Kafka broker listening on port 9092")

	// Accept and process one connection (for this challenge)
	conn, err := listener.Accept()
	if err != nil {
		log.Fatalf("Error accepting connection: %v", err)
	}

	handleConnection(conn)
}
