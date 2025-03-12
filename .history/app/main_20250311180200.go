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
	TopicNames []string
	Limit      int32
	Cursor     int8
	TagBuffer  int8
}

// DescribeTopicPartitionsResponse represents a response to a DescribeTopicPartitions request
type DescribeTopicPartitionsResponse struct {
	CorrelationID int32
	ThrottleTime  int32
	Topics        []TopicResponse
	TaggedFields  int8
}

// TopicResponse represents a single topic's response in the DescribeTopicPartitions response
type TopicResponse struct {
	Name         string
	ErrorCode    uint16
	TopicID      [16]byte // UUID (16 bytes)
	IsInternal   bool
	Partitions   []PartitionInfo
	TaggedFields int8
}

// PartitionInfo represents information about a single partition
type PartitionInfo struct {
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
	ISR            []int32 // In-Sync Replicas
	TaggedFields   int8
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
	fmt.Println("describeRequest:", describeRequest)
	if err != nil {
		fmt.Println("Error parsing DescribeTopicPartitions request:", err.Error())
		return err
	}

	fmt.Println("Processing DescribeTopicPartitions for topics:", describeRequest.TopicNames)

	// Create a structured response
	response := &DescribeTopicPartitionsResponse{
		CorrelationID: request.CorrelationID,
		ThrottleTime:  0,
		TaggedFields:  0,
		Topics:        make([]TopicResponse, 0, len(describeRequest.TopicNames)),
	}

	// Add a response for each topic
	for _, topicName := range describeRequest.TopicNames {
		// Create a null UUID (all zeros)
		var nullUUID [16]byte

		// Create topic response for unknown topic
		topicResponse := TopicResponse{
			Name:         topicName,
			ErrorCode:    ErrorUnknownTopicPartition,
			TopicID:      nullUUID,
			IsInternal:   false,
			Partitions:   []PartitionInfo{}, // Empty for unknown topics
			TaggedFields: 0,
		}

		response.Topics = append(response.Topics, topicResponse)
	}

	// Serialize the response to binary format
	responseData := SerializeDescribeTopicPartitionsResponse(response)

	// For debugging
	fmt.Println("Response size:", len(responseData))
	fmt.Println("Response hexdump:")
	for i := 0; i < len(responseData) && i < 64; i++ { // Print only first 64 bytes
		fmt.Printf("%02x ", responseData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	if len(responseData) > 64 {
		fmt.Println("... (truncated)")
	}
	fmt.Println()

	// Send the response
	return sendRawResponse(conn, responseData)
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

	offset := 0
	topicCount := int(binary.BigEndian.Uint16(payload[0:1])) - 1
	fmt.Println("topicCount:", topicCount)
	offset += 1

	topicNames := make([]string, topicCount)
	for i := range topicCount {
		topicNameLength := int8(payload[offset]) - 1
		offset += 1
		topicNames[i] = string(payload[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength) + 1 // skip tag buffer
		fmt.Println("topicNames:", topicNames[i])
	}
	limit := int32(binary.BigEndian.Uint32(payload[offset : offset+4]))
	offset += 4
	cursor := int8(payload[offset])
	offset += 1
	tagBuffer := int8(payload[offset])
	offset += 1

	return &DescribeTopicPartitionsRequest{
		TopicNames: topicNames,
		Limit:      limit,
		Cursor:     cursor,
		TagBuffer:  tagBuffer,
	}, nil
}

// SerializeDescribeTopicPartitionsResponse converts the response struct to binary format
func SerializeDescribeTopicPartitionsResponse(response *DescribeTopicPartitionsResponse) []byte {
	// Calculate the total size of the serialized response
	size := 4 // Correlation ID
	size += 4 // Throttle time
	size += 2 // Topics array length

	// Calculate size for each topic
	for _, topic := range response.Topics {
		size += 2               // Topic name length
		size += len(topic.Name) // Topic name
		size += 2               // Error code
		size += 16              // Topic ID (UUID)
		size += 1               // Is internal flag
		size += 4               // Partitions array length
		size += 1               // Tagged fields

		// We don't have any partitions for unknown topics, but
		// we'd add their size here if we did
	}

	// Allocate buffer
	buf := make([]byte, size)
	offset := 0

	// Write correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(response.CorrelationID))
	offset += 4

	// Write throttle time
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(response.ThrottleTime))
	offset += 4

	// Write topics count
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(response.Topics)))
	offset += 2

	// Write each topic
	for _, topic := range response.Topics {
		// Topic name length
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topic.Name)))
		offset += 2

		// Topic name
		copy(buf[offset:offset+len(topic.Name)], topic.Name)
		offset += len(topic.Name)

		// Error code
		binary.BigEndian.PutUint16(buf[offset:offset+2], topic.ErrorCode)
		offset += 2

		// Topic ID (UUID)
		copy(buf[offset:offset+16], topic.TopicID[:])
		offset += 16

		// Is internal flag
		if topic.IsInternal {
			buf[offset] = 1
		} else {
			buf[offset] = 0
		}
		offset += 1

		// Partitions array length (0 for unknown topics)
		binary.BigEndian.PutUint32(buf[offset:offset+4], 0)
		offset += 4

		// Tagged fields
		buf[offset] = byte(topic.TaggedFields)
		offset += 1
	}

	return buf
}
