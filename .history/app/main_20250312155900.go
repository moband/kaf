package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Kafka Protocol Constants
const (
	// API Keys
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75

	// Error codes
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35
	ErrorUnknownTopic       uint16 = 3

	// Version constraints
	MinSupportedVersion int16 = 0
	MaxSupportedVersion int16 = 4

	// API version ranges
	ApiVersionsMinVersion   int16 = 0
	ApiVersionsMaxVersion   int16 = 4
	DescribeTopicMinVersion int16 = 0
	DescribeTopicMaxVersion int16 = 0

	// Topic authorized operations bitfield value
	TopicAuthorizedOps uint32 = 0x00000df8
)

// KafkaRequest represents a Kafka protocol request
type KafkaRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	Payload       []byte
}

// KafkaResponse represents a generic Kafka protocol response
type KafkaResponse struct {
	CorrelationID int32
	ErrorCode     uint16
	Payload       []byte
}

// Protocol helper functions
func writeInt16(buffer []byte, offset int, value int16) int {
	binary.BigEndian.PutUint16(buffer[offset:offset+2], uint16(value))
	return offset + 2
}

func writeInt32(buffer []byte, offset int, value int32) int {
	binary.BigEndian.PutUint32(buffer[offset:offset+4], uint32(value))
	return offset + 4
}

func writeUint16(buffer []byte, offset int, value uint16) int {
	binary.BigEndian.PutUint16(buffer[offset:offset+2], value)
	return offset + 2
}

func writeUint32(buffer []byte, offset int, value uint32) int {
	binary.BigEndian.PutUint32(buffer[offset:offset+4], value)
	return offset + 4
}

func writeCompactString(buffer []byte, offset int, value string) int {
	// Compact string: varint length (actual length + 1) followed by string bytes
	buffer[offset] = byte(len(value) + 1)
	offset++
	copy(buffer[offset:offset+len(value)], value)
	return offset + len(value)
}

func ensureBufferCapacity(buffer []byte, requiredCapacity int) []byte {
	if len(buffer) >= requiredCapacity {
		return buffer
	}

	// Calculate new size (double current size or fit required capacity)
	newSize := len(buffer) * 2
	if newSize < requiredCapacity {
		newSize = requiredCapacity
	}

	newBuffer := make([]byte, newSize)
	copy(newBuffer, buffer)
	return newBuffer
}

func main() {
	fmt.Println("Kafka server starting...")

	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err.Error())
		os.Exit(1)
	}

	fmt.Println("Listening on port 9092")

	// Accept connections in a loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}

		fmt.Println("New connection from:", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

// handleConnection processes requests from a single client connection
func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Parse the incoming request
		request, err := readRequest(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading request:", err.Error())
			}
			return
		}

		// Log the request details
		logRequest(request)

		// Handle the request based on API key and version
		if err := handleRequest(conn, request); err != nil {
			fmt.Println("Error handling request:", err.Error())
			return
		}
	}
}

// readRequest reads and parses a Kafka request from the connection
func readRequest(conn net.Conn) (*KafkaRequest, error) {
	request := &KafkaRequest{}

	// Read message length
	if err := binary.Read(conn, binary.BigEndian, &request.Length); err != nil {
		return nil, err
	}

	// Read API key
	if err := binary.Read(conn, binary.BigEndian, &request.ApiKey); err != nil {
		return nil, err
	}

	// Read API version
	if err := binary.Read(conn, binary.BigEndian, &request.ApiVersion); err != nil {
		return nil, err
	}

	// Read correlation ID
	if err := binary.Read(conn, binary.BigEndian, &request.CorrelationID); err != nil {
		return nil, err
	}

	// Read the rest of the request payload
	request.Payload = make([]byte, request.Length-8) // 8 bytes for ApiKey, ApiVersion, and CorrelationID
	if err := binary.Read(conn, binary.BigEndian, &request.Payload); err != nil {
		return nil, err
	}

	return request, nil
}

// logRequest logs the details of a Kafka request
func logRequest(request *KafkaRequest) {
	fmt.Println("Request details:")
	fmt.Println("  Length:", request.Length)
	fmt.Println("  API Key:", request.ApiKey)
	fmt.Println("  API Version:", request.ApiVersion)
	fmt.Println("  Correlation ID:", request.CorrelationID)
}

// handleRequest routes the request to the appropriate handler based on API key
func handleRequest(conn net.Conn, request *KafkaRequest) error {
	// Check if API version is supported
	if request.ApiVersion < MinSupportedVersion || request.ApiVersion > MaxSupportedVersion {
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnsupportedVersion)
	}

	// Handle the request based on the API key
	switch request.ApiKey {
	case ApiVersionsKey:
		return handleApiVersionsRequest(conn, request)
	case DescribeTopicPartitionsKey:
		return handleDescribeTopicPartitionsRequest(conn, request)
	default:
		return handleGenericRequest(conn, request)
	}
}

// handleApiVersionsRequest handles the ApiVersions API request
func handleApiVersionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Create response for API_VERSIONS request
	response := make([]byte, 26)
	offset := 0

	// Correlation ID
	offset = writeInt32(response, offset, request.CorrelationID)

	// Error code (0 for success)
	offset = writeUint16(response, offset, ErrorNone)

	// Number of API keys in response
	response[offset] = 3
	offset++

	// API Key 1 - API_VERSIONS (18)
	offset = writeInt16(response, offset, ApiVersionsKey)
	offset = writeInt16(response, offset, ApiVersionsMinVersion)
	offset = writeInt16(response, offset, ApiVersionsMaxVersion)
	response[offset] = 0 // _tagged_fields
	offset++

	// API Key 2 - DescribeTopicPartitions (75)
	offset = writeInt16(response, offset, DescribeTopicPartitionsKey)
	offset = writeInt16(response, offset, DescribeTopicMinVersion)
	offset = writeInt16(response, offset, DescribeTopicMaxVersion)
	response[offset] = 0 // _tagged_fields
	offset++

	// Throttle time (0)
	offset = writeInt32(response, offset, 0)

	// _tagged_fields for the overall response
	response[offset] = 0

	return sendRawResponse(conn, response)
}

// skipClientID skips the client ID field in a request payload
func skipClientID(payload []byte) (int, error) {
	if len(payload) < 2 {
		return 0, fmt.Errorf("payload too short to contain client ID length")
	}

	// Client ID length (2 bytes)
	clientIDLength := int(binary.BigEndian.Uint16(payload[0:2]))
	offset := 2 + clientIDLength

	if offset >= len(payload) {
		return 0, fmt.Errorf("payload too short to contain client ID and tag buffer")
	}

	// Skip tag buffer (1 byte)
	offset++

	return offset, nil
}

// parseCompactString parses a compact string from a buffer at the given offset
func parseCompactString(payload []byte, offset int) (string, int, error) {
	if offset >= len(payload) {
		return "", 0, fmt.Errorf("unexpected end of payload when reading string length")
	}

	// Compact string format: varint length (actual length + 1) followed by string bytes
	strLength := int(payload[offset]) - 1
	offset++

	if offset+strLength > len(payload) {
		return "", 0, fmt.Errorf("unexpected end of payload when reading string content")
	}

	str := string(payload[offset : offset+strLength])
	offset += strLength

	return str, offset, nil
}

// handleDescribeTopicPartitionsRequest handles the DescribeTopicPartitions API request
func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Skip client ID in the payload
	offset, err := skipClientID(request.Payload)
	if err != nil {
		return err
	}

	// Parse topics array
	if offset >= len(request.Payload) {
		return fmt.Errorf("unexpected end of payload when reading topics array length")
	}

	// Topics array length (compact array format)
	topicArrayLength := int(request.Payload[offset]) - 1
	if topicArrayLength < 0 {
		return fmt.Errorf("invalid topics array length")
	}
	offset++

	// Extract topic names
	topics := make([]string, 0, topicArrayLength)
	for i := 0; i < topicArrayLength; i++ {
		topicName, newOffset, err := parseCompactString(request.Payload, offset)
		if err != nil {
			return err
		}
		offset = newOffset

		// Skip tag buffer if present
		if offset < len(request.Payload) && request.Payload[offset] == 0 {
			offset++
		}

		topics = append(topics, topicName)
	}

	// Build response
	return buildDescribeTopicPartitionsResponse(conn, request.CorrelationID, topics)
}

// buildDescribeTopicPartitionsResponse creates and sends a DescribeTopicPartitions response
func buildDescribeTopicPartitionsResponse(conn net.Conn, correlationID int32, topics []string) error {
	// Estimate initial response size
	responseSize := 12 + (40 * len(topics)) // Header (10 bytes) + cursor & tags (2 bytes) + topics space
	response := make([]byte, responseSize)
	offset := 0

	// Response header - Correlation ID
	offset = writeInt32(response, offset, correlationID)

	// Tag buffer for the header (empty)
	response[offset] = 0
	offset++

	// Throttle time (0)
	offset = writeInt32(response, offset, 0)

	// Topics array length (compact array encoding)
	response[offset] = byte(len(topics) + 1) // varint encoding of array length + 1
	offset++

	// Add each topic to the response
	for _, topicName := range topics {
		// Ensure we have enough buffer space for this topic
		requiredSpace := offset + 25 + len(topicName) // Rough estimate for a topic entry
		response = ensureBufferCapacity(response, requiredSpace)

		// Topic error code - UNKNOWN_TOPIC (3)
		offset = writeUint16(response, offset, ErrorUnknownTopic)

		// Topic name (compact string)
		offset = writeCompactString(response, offset, topicName)

		// Topic ID (all zeros for unknown topic - 16 bytes)
		// Buffer is already zeroed, just advance the offset
		offset += 16

		// is_internal (false)
		response[offset] = 0
		offset++

		// Partitions array (empty)
		response[offset] = 1 // varint encoding of array length + 1 (empty array)
		offset++

		// Topic authorized operations
		offset = writeUint32(response, offset, TopicAuthorizedOps)

		// Tag buffer for the topic (empty)
		response[offset] = 0
		offset++
	}

	// Next cursor (null)
	response = ensureBufferCapacity(response, offset+2)
	response[offset] = 0xff // Null cursor
	offset++

	// Tag buffer (empty)
	response[offset] = 0
	offset++

	// Trim the response to the actual size used
	return sendRawResponse(conn, response[:offset])
}

// handleGenericRequest handles any unrecognized API requests
func handleGenericRequest(conn net.Conn, request *KafkaRequest) error {
	// Simple response for other API keys
	response := &KafkaResponse{
		CorrelationID: request.CorrelationID,
		ErrorCode:     ErrorNone,
	}

	return sendResponse(conn, response)
}

// sendErrorResponse sends a minimal error response
func sendErrorResponse(conn net.Conn, correlationID int32, errorCode uint16) error {
	// Create minimal error response
	response := make([]byte, 6)
	writeInt32(response, 0, correlationID)
	writeUint16(response, 4, errorCode)

	return sendRawResponse(conn, response)
}

// sendResponse sends a structured KafkaResponse
func sendResponse(conn net.Conn, response *KafkaResponse) error {
	// Calculate response size
	responseSize := 6 // 4 bytes for correlationID + 2 bytes for errorCode
	if response.Payload != nil {
		responseSize += len(response.Payload)
	}

	// Create response buffer
	buffer := make([]byte, responseSize)
	offset := 0

	// Write correlation ID
	offset = writeInt32(buffer, offset, response.CorrelationID)

	// Write error code
	offset = writeUint16(buffer, offset, response.ErrorCode)

	// Write payload if present
	if response.Payload != nil {
		copy(buffer[offset:], response.Payload)
	}

	return sendRawResponse(conn, buffer)
}

// sendRawResponse sends the raw response bytes with proper message length
func sendRawResponse(conn net.Conn, data []byte) error {
	// Write message size
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}

	// Write message data
	if err := binary.Write(conn, binary.BigEndian, data); err != nil {
		return err
	}

	return nil
}
