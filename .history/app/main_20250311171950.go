package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Kafka protocol constants
const (
	// API Keys
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75

	// Error codes
	ErrorNone                  uint16 = 0
	ErrorUnsupportedVersion    uint16 = 35
	ErrorUnknownTopicPartition uint16 = 3

	// Version constraints
	MinSupportedVersion int16 = 0
	MaxSupportedVersion int16 = 4

	// API version ranges
	ApiVersionsMinVersion   int16 = 0
	ApiVersionsMaxVersion   int16 = 4
	DescribeTopicMinVersion int16 = 0
	DescribeTopicMaxVersion int16 = 0
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

// DescribeTopicPartitionsRequest represents a request to describe topic partitions
type DescribeTopicPartitionsRequest struct {
	// We only need to parse the topic name for this stage
	TopicName string
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

func logRequest(request *KafkaRequest) {
	fmt.Println("Request details:")
	fmt.Println("  Length:", request.Length)
	fmt.Println("  API Key:", request.ApiKey)
	fmt.Println("  API Version:", request.ApiVersion)
	fmt.Println("  Correlation ID:", request.CorrelationID)
}

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

func handleApiVersionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Create response for API_VERSIONS request
	response := make([]byte, 26)

	// Correlation ID
	binary.BigEndian.PutUint32(response[0:4], uint32(request.CorrelationID))

	// Error code (0 for success)
	binary.BigEndian.PutUint16(response[4:6], ErrorNone)

	// Number of API keys in response
	response[6] = 3

	// API Key 1 - API_VERSIONS (18)
	binary.BigEndian.PutUint16(response[7:9], uint16(ApiVersionsKey))
	binary.BigEndian.PutUint16(response[9:11], uint16(ApiVersionsMinVersion))  // Min version
	binary.BigEndian.PutUint16(response[11:13], uint16(ApiVersionsMaxVersion)) // Max version
	response[13] = 0                                                           // _tagged_fields

	// API Key 2 - DescribeTopicPartitions (75)
	binary.BigEndian.PutUint16(response[14:16], uint16(DescribeTopicPartitionsKey))
	binary.BigEndian.PutUint16(response[16:18], uint16(DescribeTopicMinVersion)) // Min version
	binary.BigEndian.PutUint16(response[18:20], uint16(DescribeTopicMaxVersion)) // Max version
	response[20] = 0                                                             // _tagged_fields

	// Throttle time
	binary.BigEndian.PutUint32(response[21:25], 0)

	// _tagged_fields for the overall response
	response[25] = 0

	return sendRawResponse(conn, response)
}

func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Parse the request payload
	describeRequest, err := parseDescribeTopicPartitionsRequest(request.Payload)
	if err != nil {
		fmt.Println("Error parsing DescribeTopicPartitions request:", err.Error())
		return err
	}

	fmt.Println("Processing DescribeTopicPartitions for topic:", describeRequest.TopicName)

	// Build response for unknown topic
	// Response format based on DescribeTopicPartitions Response (v0) for unknown topic

	// Calculate sizes
	topicNameLength := len(describeRequest.TopicName)

	// Total size:
	// 4 bytes for correlationID
	// 2 bytes for throttle time
	// 2 bytes for topic count (1)
	// 2 bytes for topic name length
	// N bytes for topic name
	// 2 bytes for error code
	// 16 bytes for topic ID (all zeros)
	// 4 bytes for tagged fields
	// 4 bytes for empty partitions array
	totalSize := 4 + 2 + 2 + 2 + topicNameLength + 2 + 16 + 4 + 4

	response := make([]byte, totalSize)
	offset := 0

	// Correlation ID
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(request.CorrelationID))
	offset += 4

	// Throttle time (0)
	binary.BigEndian.PutUint32(response[offset:offset+4], 0)
	offset += 4

	// Topics array length (1)
	binary.BigEndian.PutUint16(response[offset:offset+2], 1)
	offset += 2

	// Topic name length
	binary.BigEndian.PutUint16(response[offset:offset+2], uint16(topicNameLength))
	offset += 2

	// Topic name
	copy(response[offset:offset+topicNameLength], describeRequest.TopicName)
	offset += topicNameLength

	// Error code (UNKNOWN_TOPIC_OR_PARTITION)
	binary.BigEndian.PutUint16(response[offset:offset+2], ErrorUnknownTopicPartition)
	offset += 2

	// Topic ID (all zeros: 00000000-0000-0000-0000-000000000000)
	// 16 bytes of zeros
	for i := 0; i < 16; i++ {
		response[offset+i] = 0
	}
	offset += 16

	// Tagged fields (empty)
	binary.BigEndian.PutUint32(response[offset:offset+4], 0)
	offset += 4

	// Partitions array (empty)
	binary.BigEndian.PutUint32(response[offset:offset+4], 0) // Empty array

	return sendRawResponse(conn, response)
}

func handleGenericRequest(conn net.Conn, request *KafkaRequest) error {
	// Simple response for other API keys
	response := &KafkaResponse{
		CorrelationID: request.CorrelationID,
		ErrorCode:     ErrorNone,
	}

	return sendResponse(conn, response)
}

func sendErrorResponse(conn net.Conn, correlationID int32, errorCode uint16) error {
	// Create minimal error response
	response := make([]byte, 6)
	binary.BigEndian.PutUint32(response[0:4], uint32(correlationID))
	binary.BigEndian.PutUint16(response[4:6], errorCode)

	return sendRawResponse(conn, response)
}

func sendResponse(conn net.Conn, response *KafkaResponse) error {
	// Calculate response size
	responseSize := 6 // 4 bytes for correlationID + 2 bytes for errorCode
	if response.Payload != nil {
		responseSize += len(response.Payload)
	}

	// Create response buffer
	buffer := make([]byte, responseSize)

	// Write correlation ID
	binary.BigEndian.PutUint32(buffer[0:4], uint32(response.CorrelationID))

	// Write error code
	binary.BigEndian.PutUint16(buffer[4:6], response.ErrorCode)

	// Write payload if present
	if response.Payload != nil {
		copy(buffer[6:], response.Payload)
	}

	return sendRawResponse(conn, buffer)
}

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

// parseDescribeTopicPartitionsRequest parses a DescribeTopicPartitions request payload
func parseDescribeTopicPartitionsRequest(payload []byte) (*DescribeTopicPartitionsRequest, error) {
	if len(payload) < 2 { // Not enough data to even get topic count
		return nil, fmt.Errorf("invalid payload: too short")
	}

	// Skip first 2 bytes (topic count) - we're only handling one topic in this stage
	// Read the string length (2 bytes)
	topicNameLength := int(binary.BigEndian.Uint16(payload[2:4]))

	if len(payload) < 4+topicNameLength {
		return nil, fmt.Errorf("invalid payload: topic name truncated")
	}

	// Read the topic name (variable length)
	topicName := string(payload[4 : 4+topicNameLength])

	return &DescribeTopicPartitionsRequest{
		TopicName: topicName,
	}, nil
}
