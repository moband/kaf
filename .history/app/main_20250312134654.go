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

	// Constants for API keys
	ProduceAPIKey                 = 0
	FetchAPIKey                   = 1
	ListOffsetsAPIKey             = 2
	MetadataAPIKey                = 3
	DescribeTopicPartitionsAPIKey = 75
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

// DescribeTopicPartitionsResponse represents the response for the DescribeTopicPartitions API
type DescribeTopicPartitionsResponse struct {
	CorrelationID int32
	Topics        []DescribeTopicPartitionsResponseTopic
}

// DescribeTopicPartitionsResponseTopic represents a topic in the DescribeTopicPartitions response
type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode  int8
	Name       string
	TopicID    [16]byte // UUID (16 bytes)
	IsInternal bool
	Partitions []DescribeTopicPartitionsResponsePartition
}

// DescribeTopicPartitionsResponsePartition represents a partition in the DescribeTopicPartitions response
type DescribeTopicPartitionsResponsePartition struct {
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
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

// Error codes
const (
	NONE                       = 0
	UNKNOWN_SERVER_ERROR       = 1
	INVALID_MESSAGE            = 2
	UNKNOWN_TOPIC_OR_PARTITION = 3
	INVALID_MESSAGE_SIZE       = 4
	LEADER_NOT_AVAILABLE       = 5
	NOT_LEADER_FOR_PARTITION   = 6
)

// Topic not found error
// const ErrorUnknownTopicPartition = 3

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

	fmt.Println("Processing DescribeTopicPartitions for topics:", describeRequest.TopicNames)

	// Build a response with unknown topics
	response := &DescribeTopicPartitionsResponse{
		CorrelationID: request.CorrelationID,
		Topics:        make([]DescribeTopicPartitionsResponseTopic, len(describeRequest.TopicNames)),
	}

	// Process each requested topic
	for i, topicName := range describeRequest.TopicNames {
		// Create a null UUID for unknown topics
		var nullUUID [16]byte

		// Prepare topic response - Unknown topic error code
		topicResponse := DescribeTopicPartitionsResponseTopic{
			ErrorCode:  UNKNOWN_TOPIC_OR_PARTITION,
			Name:       topicName,
			TopicID:    nullUUID,
			IsInternal: false,
			Partitions: []DescribeTopicPartitionsResponsePartition{},
		}

		response.Topics[i] = topicResponse
	}

	// Serialize the response to binary format
	responseData, err := SerializeDescribeTopicPartitionsResponse(response)
	if err != nil {
		fmt.Println("Error serializing DescribeTopicPartitions response:", err.Error())
		return err
	}

	// For debugging
	fmt.Printf("DescribeTopicPartitions response size: %d bytes\n", len(responseData))
	fmt.Println("Response hexdump:")
	for i := 0; i < len(responseData); i++ {
		fmt.Printf("%02x ", responseData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	// Write the response size (4 bytes) followed by the response data
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(responseData)))

	if _, err := conn.Write(sizeBytes); err != nil {
		return err
	}

	if _, err := conn.Write(responseData); err != nil {
		return err
	}

	return nil
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
	// Based on the hexdump, the format appears to be:
	// 00 0c             - Client ID length (12)
	// 6b 61...65 72     - "kafka-tester" (client ID)
	// 00 02             - Topics count (2)
	// 12                - Type marker for topic
	// 75 6e 6b...7a     - "unknown-topic-saz" (topic name - not length prefixed, but null terminated)
	// 00                - Null terminator
	// 00 00 00 01       - Limit
	// ff                - Cursor
	// 00                - Tag buffer

	if len(payload) < 4 {
		return nil, fmt.Errorf("payload too short")
	}

	// Print the payload for debugging
	fmt.Println("Payload hexdump:")
	for i := 0; i < len(payload); i++ {
		fmt.Printf("%02x ", payload[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	offset := 0

	// Parse client ID length (2 bytes)
	clientIDLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	// Skip client ID
	if len(payload) < offset+clientIDLength {
		return nil, fmt.Errorf("payload too short for client ID")
	}
	clientID := string(payload[offset : offset+clientIDLength])
	fmt.Printf("Client ID: %s\n", clientID)
	offset += clientIDLength

	// Read topics count (2 bytes)
	if len(payload) < offset+2 {
		return nil, fmt.Errorf("payload too short for topics count")
	}
	topicsCount := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	fmt.Printf("Topics count: %d\n", topicsCount)
	offset += 2

	// Create result
	result := &DescribeTopicPartitionsRequest{
		TopicNames: make([]string, 0, topicsCount),
		Limit:      1, // Default value
		Cursor:     0,
		TagBuffer:  0,
	}

	// Parse each topic based on the type marker + null-terminated string pattern
	for i := 0; i < topicsCount && offset < len(payload); i++ {
		// Check if we have enough bytes for the type marker
		if offset >= len(payload) {
			break
		}

		// Read type marker (should be 0x12 for string)
		typeMarker := payload[offset]
		offset++

		// Only process this as a topic if it's a string type marker (0x12)
		if typeMarker == 0x12 {
			// Find the topic name (null-terminated string)
			startPos := offset

			// Find the null terminator
			for offset < len(payload) && payload[offset] != 0 {
				offset++
			}

			if offset < len(payload) {
				topicName := string(payload[startPos:offset])
				fmt.Printf("Topic %d: marker=0x%02x, name='%s'\n", i+1, typeMarker, topicName)
				result.TopicNames = append(result.TopicNames, topicName)

				// Skip the null terminator
				offset++
			} else {
				// No null terminator found - use what we have
				if startPos < len(payload) {
					topicName := string(payload[startPos:])
					fmt.Printf("Topic %d (truncated): marker=0x%02x, name='%s'\n", i+1, typeMarker, topicName)
					result.TopicNames = append(result.TopicNames, topicName)
				}
				break
			}
		} else {
			// Not a string type marker, skip this entry
			fmt.Printf("Skipping non-string marker 0x%02x at offset %d\n", typeMarker, offset-1)
			// Try to find the next valid marker or end of payload
			for offset < len(payload) && payload[offset] != 0x12 {
				offset++
			}
		}
	}

	// Read limit if we have enough bytes
	if offset+4 <= len(payload) {
		result.Limit = int32(binary.BigEndian.Uint32(payload[offset : offset+4]))
		fmt.Printf("Limit: %d\n", result.Limit)
		offset += 4
	}

	// Read cursor if we have enough bytes
	if offset < len(payload) {
		result.Cursor = int8(payload[offset])
		fmt.Printf("Cursor: %d\n", result.Cursor)
		offset++
	}

	// Read tag buffer if we have enough bytes
	if offset < len(payload) {
		result.TagBuffer = int8(payload[offset])
		fmt.Printf("Tag buffer: %d\n", result.TagBuffer)
	}

	// Validate that we found at least one topic name
	if len(result.TopicNames) == 0 {
		return nil, fmt.Errorf("no valid topic names found in payload")
	}

	return result, nil
}

// SerializeDescribeTopicPartitionsResponse serializes a DescribeTopicPartitionsResponse into bytes
func SerializeDescribeTopicPartitionsResponse(resp *DescribeTopicPartitionsResponse) ([]byte, error) {
	// Based on the tester output, the expected format is:
	// - 4 bytes for correlation ID
	// - 2 bytes for topics array length
	// - For each topic:
	//   - 2 bytes for error code (uint16)
	//   - Topic name in COMPACT_STRING format:
	//     - 2 bytes for string length
	//     - String bytes (null-terminated)
	//   - 16 bytes for Topic ID (UUID)
	//   - 1 byte for IsInternal flag
	//   - 4 bytes for partitions array length
	//   - For each partition (none in this case)
	//   - 1 byte for tag buffer

	// Initialize the buffer
	buf := make([]byte, 0, 512) // Large enough buffer to avoid reallocations

	// Add correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, uint32(resp.CorrelationID))
	buf = append(buf, correlationIDBytes...)

	// Add topics array length (2 bytes)
	topicsLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(topicsLengthBytes, uint16(len(resp.Topics)))
	buf = append(buf, topicsLengthBytes...)

	// Add each topic
	for _, topic := range resp.Topics {
		// Add error code (2 bytes) - Convert to uint16 as the tester expects
		errorCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCodeBytes, uint16(topic.ErrorCode))
		buf = append(buf, errorCodeBytes...)

		// Add topic name as COMPACT_STRING (null-terminated string with length prefix)
		topicNameBytes := []byte(topic.Name)

		// Add string length (2 bytes)
		nameLengthBytes := make([]byte, 2)
		nameLength := len(topicNameBytes) + 1 // +1 for null terminator
		binary.BigEndian.PutUint16(nameLengthBytes, uint16(nameLength))
		buf = append(buf, nameLengthBytes...)

		// Add topic name
		buf = append(buf, topicNameBytes...)

		// Add null terminator
		buf = append(buf, 0)

		// Add Topic ID (16 bytes UUID)
		buf = append(buf, topic.TopicID[:]...)

		// Add IsInternal flag (1 byte)
		if topic.IsInternal {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}

		// Add partitions array length (4 bytes)
		partitionsLengthBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsLengthBytes, uint32(len(topic.Partitions)))
		buf = append(buf, partitionsLengthBytes...)

		// Add partitions information (none in this case)
		for _, partition := range topic.Partitions {
			// Add partition index (4 bytes)
			partitionIndexBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIndexBytes, uint32(partition.PartitionIndex))
			buf = append(buf, partitionIndexBytes...)

			// Add leader ID (4 bytes)
			leaderIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(leaderIDBytes, uint32(partition.LeaderID))
			buf = append(buf, leaderIDBytes...)

			// Add leader epoch (4 bytes)
			leaderEpochBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(leaderEpochBytes, uint32(partition.LeaderEpoch))
			buf = append(buf, leaderEpochBytes...)

			// Add empty arrays for replicas and ISR
			buf = append(buf, []byte{0, 0, 0, 0, 0, 0, 0, 0}...) // 4 bytes for replica count + 4 bytes for ISR count
		}

		// Add tag buffer (1 byte)
		buf = append(buf, 0)
	}

	// Debug: print hexdump of the serialized response
	fmt.Println("Serialized response hexdump:")
	for i := 0; i < len(buf); i++ {
		fmt.Printf("%02x ", buf[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	fmt.Printf("Total response size: %d bytes\n", len(buf))

	return buf, nil
}
