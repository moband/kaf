package kafka

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/internal/kafka/protocol"
	"github.com/codecrafters-io/kafka-starter-go/pkg/logger"
)

// MessageParser reads and parses Kafka protocol messages from a connection
type MessageParser struct {
	logger *logger.Logger
}

// NewMessageParser creates a new message parser
func NewMessageParser(logger *logger.Logger) *MessageParser {
	return &MessageParser{
		logger: logger,
	}
}

// ReadRequest reads a Kafka protocol request from a connection
func (p *MessageParser) ReadRequest(conn net.Conn) (*protocol.Request, error) {
	request := &protocol.Request{}

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
	if _, err := io.ReadFull(conn, request.Payload); err != nil {
		return nil, err
	}

	p.logRequest(request)

	return request, nil
}

// logRequest logs the details of a Kafka protocol request
func (p *MessageParser) logRequest(request *protocol.Request) {
	p.logger.Debug("Request details: Length=%d, ApiKey=%d, ApiVersion=%d, CorrelationID=%d",
		request.Length, request.ApiKey, request.ApiVersion, request.CorrelationID)
}
