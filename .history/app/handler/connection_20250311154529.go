package handler

import (
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

// ConnectionHandler handles client connections
type ConnectionHandler struct {
	config *config.Config
	logger *utils.Logger
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(cfg *config.Config, logger *utils.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		config: cfg,
		logger: logger,
	}
}

// HandleConnection handles a client connection
func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
	defer func() {
		h.logger.Infof("Closing connection from %s", conn.RemoteAddr())
		conn.Close()
	}()

	h.logger.Infof("Accepted connection from %s", conn.RemoteAddr())

	for {
		// Read request header
		header, err := kafka.ReadRequestHeader(conn)
		if err != nil {
			h.logger.Errorf("Error reading request header: %v", err)
			return
		}

		h.logger.Debugf("Received request: size=%d, apiKey=%d, apiVersion=%d, correlationID=%d",
			header.Size, header.APIKey, header.APIVersion, header.CorrelationID)

		// Handle the request based on the API key
		switch header.APIKey {
		case kafka.APIKeyAPIVersions:
			h.handleAPIVersionsRequest(conn, header)
		default:
			h.logger.Warnf("Unsupported API key: %d", header.APIKey)
			kafka.WriteErrorResponse(conn, header.CorrelationID, kafka.ErrorCodeUnsupportedVersion)
		}
	}
}

// handleAPIVersionsRequest handles an API versions request
func (h *ConnectionHandler) handleAPIVersionsRequest(conn net.Conn, header *kafka.RequestHeader) {
	// Check if API version is supported
	if !h.config.IsAPIVersionSupported(header.APIKey, header.APIVersion) {
		h.logger.Warnf("Unsupported API version: %d for API key: %d", header.APIVersion, header.APIKey)
		kafka.WriteErrorResponse(conn, header.CorrelationID, kafka.ErrorCodeUnsupportedVersion)
		return
	}

	// Create response
	response := &kafka.APIVersionsResponse{
		Header: kafka.ResponseHeader{
			CorrelationID: header.CorrelationID,
			ErrorCode:     kafka.ErrorCodeNone,
		},
		APIKeys: []kafka.APIKey{
			{
				APIKey:     kafka.APIKeyAPIVersions,
				MinVersion: 3,
				MaxVersion: 4,
			},
		},
		ThrottleTime: 0,
	}

	// Write response
	if err := kafka.WriteAPIVersionsResponse(conn, response); err != nil {
		h.logger.Errorf("Error writing API versions response: %v", err)
		return
	}

	h.logger.Debugf("Sent API versions response to %s", conn.RemoteAddr())
}
