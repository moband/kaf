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
	if requestApiVersion < 0 || requestApiVersion > 4 {
		// Error response for unsupported version
		errorResponse := make([]byte, 6)
		binary.BigEndian.PutUint32(errorResponse, uint32(correlationId))
		binary.BigEndian.PutUint16(errorResponse[4:], ErrorUnsupportedVersion)
		send(conn, errorResponse)
		return
	}

	// The ApiVersions v3/v4 response format:
	// - error_code: INT16 (2 bytes)
	// - api_versions array:
	//   - array length: INT32 (4 bytes)
	//   - entries: each with api_key (2 bytes), min_version (2 bytes), max_version (2 bytes)
	// - throttle_time_ms: INT32 (4 bytes)
	// - tagged_fields: COMPACT_ARRAY (1 byte if empty)

	// Create response buffer - 13 bytes:
	// 2 bytes error_code + 4 bytes array length + 6 bytes API entry + 4 bytes throttle time + 1 byte tagged fields
	out := make([]byte, 17)
	idx := 0

	// Error code (2 bytes) - set to 0 (no error)
	binary.BigEndian.PutUint16(out[idx:idx+2], ErrorNone)
	idx += 2

	// API versions array
	// Number of API keys (4 bytes) - just one entry (1)
	binary.BigEndian.PutUint32(out[idx:idx+4], 1)
	idx += 4

	// API key entry for ApiVersions
	binary.BigEndian.PutUint16(out[idx:idx+2], ApiVersionsKey) // API key (18)
	idx += 2
	binary.BigEndian.PutUint16(out[idx:idx+2], 0) // Min version (0)
	idx += 2
	binary.BigEndian.PutUint16(out[idx:idx+2], 4) // Max version (4)
	idx += 2

	// Throttle time (4 bytes)
	binary.BigEndian.PutUint32(out[idx:idx+4], 0)
	idx += 4

	// Tagged fields (1 byte) - no tagged fields
	out[idx] = 0
	idx++

	// Send the response with correlation ID prepended
	response := make([]byte, 4+len(out))
	binary.BigEndian.PutUint32(response[0:4], uint32(correlationId))
	copy(response[4:], out)

	send(conn, response)
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
