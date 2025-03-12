// Package server provides the TCP server implementation for Kafka
package server

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/internal/kafka"
	"github.com/codecrafters-io/kafka-starter-go/pkg/logger"
)

// Config holds server configuration
type Config struct {
	Host       string
	Port       int
	MaxClients int
}

// Server represents a Kafka server
type Server struct {
	config    Config
	logger    *logger.Logger
	listener  net.Listener
	parser    *kafka.MessageParser
	handler   *kafka.RequestHandler
	wg        sync.WaitGroup
	clients   map[string]net.Conn
	clientsMu sync.Mutex
	shutdown  chan struct{}
}

// New creates a new Kafka server
func New(config Config, logger *logger.Logger) *Server {
	parser := kafka.NewMessageParser(logger)
	handler := kafka.NewRequestHandler(logger)

	return &Server{
		config:   config,
		logger:   logger,
		parser:   parser,
		handler:  handler,
		clients:  make(map[string]net.Conn),
		shutdown: make(chan struct{}),
	}
}

// Start starts the Kafka server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", addr, err)
	}

	s.listener = listener
	s.logger.Info("Kafka server started on %s", addr)

	// Accept connections in a goroutine
	s.wg.Add(1)
	go s.acceptConnections()

	return nil
}

// Stop stops the Kafka server
func (s *Server) Stop() error {
	// Signal the shutdown
	close(s.shutdown)

	// Close the listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Error closing listener: %s", err.Error())
		}
	}

	// Close all client connections
	s.clientsMu.Lock()
	for _, conn := range s.clients {
		if err := conn.Close(); err != nil {
			s.logger.Error("Error closing client connection: %s", err.Error())
		}
	}
	s.clientsMu.Unlock()

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.logger.Info("Kafka server stopped")
	return nil
}

// acceptConnections accepts incoming connections
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		// Check if we're shutting down
		select {
		case <-s.shutdown:
			return
		default:
			// Continue accepting
		}

		conn, err := s.listener.Accept()
		if err != nil {
			// Check if the server is shutting down
			select {
			case <-s.shutdown:
				return
			default:
				s.logger.Error("Error accepting connection: %s", err.Error())
				continue
			}
		}

		// Register the client
		clientAddr := conn.RemoteAddr().String()
		s.registerClient(clientAddr, conn)

		// Handle the connection in a goroutine
		s.wg.Add(1)
		go s.handleConnection(clientAddr, conn)
	}
}

// registerClient registers a client connection
func (s *Server) registerClient(addr string, conn net.Conn) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	s.clients[addr] = conn
	s.logger.Info("New connection from: %s", addr)
}

// unregisterClient removes a client connection
func (s *Server) unregisterClient(addr string) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	delete(s.clients, addr)
	s.logger.Info("Connection closed: %s", addr)
}

// handleConnection handles a client connection
func (s *Server) handleConnection(addr string, conn net.Conn) {
	defer func() {
		conn.Close()
		s.unregisterClient(addr)
		s.wg.Done()
	}()

	for {
		// Check if we're shutting down
		select {
		case <-s.shutdown:
			return
		default:
			// Continue handling
		}

		// Parse the incoming request
		request, err := s.parser.ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				s.logger.Error("Error reading request from %s: %s", addr, err.Error())
			}
			return
		}

		// Handle the request
		if err := s.handler.HandleRequest(conn, request); err != nil {
			s.logger.Error("Error handling request from %s: %s", addr, err.Error())
			return
		}
	}
}
