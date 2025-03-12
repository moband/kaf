// Package kafka provides the core Kafka server functionality
package kafka

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/internal/kafka/protocol"
	"github.com/codecrafters-io/kafka-starter-go/pkg/logger"
)

// RequestHandler handles incoming Kafka protocol requests
type RequestHandler struct {
	logger *logger.Logger
}

// NewRequestHandler creates a new request handler
func NewRequestHandler(logger *logger.Logger) *RequestHandler {
	return &RequestHandler{
		logger: logger,
	}
}

// HandleRequest processes a Kafka protocol request and sends the appropriate response
func (h *RequestHandler) HandleRequest(conn net.Conn, req *protocol.Request) error {
	// Check if API version is supported
	if req.ApiVersion < protocol.MinSupportedVersion || req.ApiVersion > protocol.MaxSupportedVersion {
		return h.sendErrorResponse(conn, req.CorrelationID, protocol.ErrorUnsupportedVersion)
	}

	// Handle the request based on the API key
	switch req.ApiKey {
	case protocol.ApiVersionsKey:
		return h.handleApiVersionsRequest(conn, req)
	case protocol.DescribeTopicPartitionsKey:
		return h.handleDescribeTopicPartitionsRequest(conn, req)
	default:
		return h.handleGenericRequest(conn, req)
	}
}

// handleApiVersionsRequest handles API_VERSIONS requests
func (h *RequestHandler) handleApiVersionsRequest(conn net.Conn, req *protocol.Request) error {
	// Create response for API_VERSIONS request
	response := make([]byte, 26)

	// Correlation ID
	binary.BigEndian.PutUint32(response[0:4], uint32(req.CorrelationID))

	// Error code (0 for success)
	binary.BigEndian.PutUint16(response[4:6], protocol.ErrorNone)

	// Number of API keys in response
	response[6] = 3

	// API Key 1 - API_VERSIONS (18)
	binary.BigEndian.PutUint16(response[7:9], uint16(protocol.ApiVersionsKey))
	binary.BigEndian.PutUint16(response[9:11], uint16(protocol.ApiVersionsMinVersion))  // Min version
	binary.BigEndian.PutUint16(response[11:13], uint16(protocol.ApiVersionsMaxVersion)) // Max version
	response[13] = 0                                                                    // _tagged_fields

	// API Key 2 - DescribeTopicPartitions (75)
	binary.BigEndian.PutUint16(response[14:16], uint16(protocol.DescribeTopicPartitionsKey))
	binary.BigEndian.PutUint16(response[16:18], uint16(protocol.DescribeTopicMinVersion)) // Min version
	binary.BigEndian.PutUint16(response[18:20], uint16(protocol.DescribeTopicMaxVersion)) // Max version
	response[20] = 0                                                                      // _tagged_fields

	// Throttle time
	binary.BigEndian.PutUint32(response[21:25], 0)

	// _tagged_fields for the overall response
	response[25] = 0

	return h.sendRawResponse(conn, response)
}

// handleDescribeTopicPartitionsRequest handles DESCRIBE_TOPIC_PARTITIONS requests
func (h *RequestHandler) handleDescribeTopicPartitionsRequest(conn net.Conn, req *protocol.Request) error {
	// Parse the request
	// The first part of the payload contains the client ID (string) followed by a tag buffer
	offset := 0

	// Skip the client ID
	if offset >= len(req.Payload) {
		return fmt.Errorf("unexpected end of payload when reading client ID length")
	}

	// Client ID is a string with 2-byte length prefix
	clientIDLength := int(binary.BigEndian.Uint16(req.Payload[offset : offset+2]))
	offset += 2 + clientIDLength

	// Skip the client ID tag buffer (1 byte)
	if offset >= len(req.Payload) {
		return fmt.Errorf("unexpected end of payload when reading client ID tag buffer")
	}
	offset++

	// Now we're at the topics array
	// First byte indicates the array length (varint encoding: actual length + 1)
	if offset >= len(req.Payload) {
		return fmt.Errorf("unexpected end of payload when reading topics array length")
	}

	topicArrayLength := int(req.Payload[offset]) - 1
	if topicArrayLength < 0 {
		return fmt.Errorf("invalid topics array length")
	}
	offset++

	// Start building the response
	// Estimate size - we'll resize later if needed
	responseSize := 12 + (40 * topicArrayLength) // rough estimate: header + topics array
	response := make([]byte, responseSize)

	// Response header
	// Correlation ID
	binary.BigEndian.PutUint32(response[0:4], uint32(req.CorrelationID))

	// Tag buffer for the header (empty)
	response[4] = 0

	// Throttle time (0)
	binary.BigEndian.PutUint32(response[5:9], 0)

	// Topics array length (compact array encoding)
	response[9] = byte(topicArrayLength + 1) // varint encoding of array length + 1

	// Parse and process each topic in the request
	responseOffset := 10 // Start after the topics array length in response

	for i := 0; i < topicArrayLength; i++ {
		// Extract the topic name length (varint)
		if offset >= len(req.Payload) {
			return fmt.Errorf("unexpected end of payload when reading topic name length")
		}

		topicNameLength := int(req.Payload[offset]) - 1
		offset++

		// Extract the topic name
		if offset+topicNameLength > len(req.Payload) {
			return fmt.Errorf("invalid topic name length or unexpected end of payload")
		}

		topicName := string(req.Payload[offset : offset+topicNameLength])
		offset += topicNameLength

		// Skip any tag buffer at the end of the topic entry
		if offset < len(req.Payload) && req.Payload[offset] == 0 {
			offset++
		}

		// Now add this topic to the response

		// Topic error code - UNKNOWN_TOPIC (3)
		if responseOffset+2 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		binary.BigEndian.PutUint16(response[responseOffset:responseOffset+2], protocol.ErrorUnknownTopic)
		responseOffset += 2

		// Topic name (compact string)
		if responseOffset+1+len(topicName) > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = byte(len(topicName) + 1) // varint encoding of string length + 1
		responseOffset++
		copy(response[responseOffset:responseOffset+len(topicName)], topicName)
		responseOffset += len(topicName)

		// Topic ID (all zeros for unknown topic - 16 bytes)
		if responseOffset+16 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		// No need to set these explicitly as the buffer is already zeroed
		responseOffset += 16

		// is_internal (false)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 0
		responseOffset++

		// Partitions array (empty)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 1 // varint encoding of array length + 1 (empty array)
		responseOffset++

		// Topic authorized operations
		if responseOffset+4 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		binary.BigEndian.PutUint32(response[responseOffset:responseOffset+4], 0x00000df8)
		responseOffset += 4

		// Tag buffer for the topic (empty)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 0
		responseOffset++
	}

	// Skip the response partition limit and cursor in the request
	// (We don't need these values for the response)

	// Next cursor (null)
	if responseOffset+1 > len(response) {
		// Expand response buffer if needed
		newResponse := make([]byte, len(response)*2)
		copy(newResponse, response)
		response = newResponse
	}
	response[responseOffset] = 0xff
	responseOffset++

	// Tag buffer (empty)
	if responseOffset+1 > len(response) {
		// Expand response buffer if needed
		newResponse := make([]byte, len(response)*2)
		copy(newResponse, response)
		response = newResponse
	}
	response[responseOffset] = 0
	responseOffset++

	// Trim the response to the actual size used
	return h.sendRawResponse(conn, response[:responseOffset])
}

// handleGenericRequest handles any other request type
func (h *RequestHandler) handleGenericRequest(conn net.Conn, req *protocol.Request) error {
	// Simple response for other API keys
	resp := &protocol.Response{
		CorrelationID: req.CorrelationID,
		ErrorCode:     protocol.ErrorNone,
	}

	return h.sendResponse(conn, resp)
}

// sendErrorResponse sends an error response back to the client
func (h *RequestHandler) sendErrorResponse(conn net.Conn, correlationID int32, errorCode uint16) error {
	// Create minimal error response
	response := make([]byte, 6)
	binary.BigEndian.PutUint32(response[0:4], uint32(correlationID))
	binary.BigEndian.PutUint16(response[4:6], errorCode)

	return h.sendRawResponse(conn, response)
}

// sendResponse sends a structured response back to the client
func (h *RequestHandler) sendResponse(conn net.Conn, response *protocol.Response) error {
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

	return h.sendRawResponse(conn, buffer)
}

// sendRawResponse sends raw byte data back to the client
func (h *RequestHandler) sendRawResponse(conn net.Conn, data []byte) error {
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
