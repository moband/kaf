package config

import (
	"os"
	"strconv"
)

// Config holds all configuration for the application
type Config struct {
	// Server configuration
	Host string
	Port int

	// Kafka-specific configuration
	SupportedAPIVersions map[int16]struct {
		MinVersion int16
		MaxVersion int16
	}
}

// NewConfig creates a new configuration with default values
func NewConfig() *Config {
	cfg := &Config{
		Host: "0.0.0.0",
		Port: 9092,
		SupportedAPIVersions: map[int16]struct {
			MinVersion int16
			MaxVersion int16
		}{
			18: {MinVersion: 3, MaxVersion: 4}, // API_VERSIONS
			75: {MinVersion: 0, MaxVersion: 0}, // Other API
		},
	}

	// Override with environment variables if present
	if host := os.Getenv("KAFKA_HOST"); host != "" {
		cfg.Host = host
	}

	if portStr := os.Getenv("KAFKA_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			cfg.Port = port
		}
	}

	return cfg
}

// GetListenAddress returns the full listen address as a string
func (c *Config) GetListenAddress() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}

// IsAPIVersionSupported checks if the API key and version are supported
func (c *Config) IsAPIVersionSupported(apiKey, apiVersion int16) bool {
	apiInfo, exists := c.SupportedAPIVersions[apiKey]
	if !exists {
		return false
	}
	return apiVersion >= apiInfo.MinVersion && apiVersion <= apiInfo.MaxVersion
}
