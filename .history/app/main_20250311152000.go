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

	// Read and process the request
	correlationID, apiVersion, err := readKafkaRequest(conn)
	if err != nil {
		log.Printf("Error processing request: %v", err)
		return
	}

	// Determine if the API version is supported
	errorCode := ErrorNone
	if apiVersion > MaxSupportedVersion {
		errorCode = ErrorUnsupportedVersion
	}

	// Send response
	if err := sendApiVersionsResponse(conn, correlationID, errorCode); err != nil {
		log.Printf("Error sending response: %v", err)
	}
}

// readKafkaRequest reads and parses a Kafka request, returning the correlation ID and API version
func readKafkaRequest(conn net.Conn) (correlationID uint32, apiVersion uint16, err error) {
	// Read message size (first 4 bytes)
	messageSizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, messageSizeBytes); err != nil {
		return 0, 0, fmt.Errorf("failed to read message size: %w", err)
	}
	messageSize := binary.BigEndian.Uint32(messageSizeBytes)

	// Read the message body
	messageBody := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, messageBody); err != nil {
		return 0, 0, fmt.Errorf("failed to read message body: %w", err)
	}

	// Extract API version (bytes 2-4 of message body)
	apiVersion = binary.BigEndian.Uint16(messageBody[2:4])

	// Extract correlation ID (bytes 4-8 of message body)
	correlationID = binary.BigEndian.Uint32(messageBody[4:8])

	return correlationID, apiVersion, nil
}

// sendApiVersionsResponse sends a Kafka ApiVersions response
func sendApiVersionsResponse(conn net.Conn, correlationID uint32, errorCode uint16) error {
	// ApiVersions v3/v4 response format:
	// Header: 4 bytes message_size + 4 bytes correlation_id
	// Body:
	//  - 2 bytes error_code
	//  - 4 bytes throttle_time_ms
	//  - 2 bytes array length (1)
	//  - API Versions entries:
	//    - 2 bytes api_key (18)
	//    - 2 bytes min_version (0)
	//    - 2 bytes max_version (4)
	//  - 1 byte tagged fields (0)

	// Create the exact response we need byte by byte
	response := []byte{
		0, 0, 0, 15, // message size (15 bytes)
		0, 0, 0, 0, // correlation ID (to be filled)
		0, 0, // error code (to be filled)
		0, 0, 0, 0, // throttle time (0)
		0, 1, // array length (1)
		0, 18, // API key (18)
		0, 0, // min version (0)
		0, 4, // max version (4)
		0, // tagged fields (0)
	}

	// Fill in the correlation ID
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	// Fill in the error code
	binary.BigEndian.PutUint16(response[8:10], errorCode)

	// Send response
	if _, err := conn.Write(response); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
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
