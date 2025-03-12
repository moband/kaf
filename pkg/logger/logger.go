// Package logger provides logging functionality for the Kafka server
package logger

import (
	"fmt"
	"os"
	"time"
)

// Level defines the severity level of the log
type Level int

const (
	// DEBUG level logs detailed information for debugging
	DEBUG Level = iota
	// INFO level logs informational messages
	INFO
	// ERROR level logs error messages
	ERROR
)

var levelNames = map[Level]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	ERROR: "ERROR",
}

// Logger is the interface for logging messages
type Logger struct {
	minLevel Level
}

// New creates a new logger with the specified minimum level
func New(level Level) *Logger {
	return &Logger{
		minLevel: level,
	}
}

// Log logs a message with the specified level
func (l *Logger) Log(level Level, format string, args ...interface{}) {
	if level < l.minLevel {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stdout, "[%s] %s: %s\n", timestamp, levelNames[level], message)
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	l.Log(DEBUG, format, args...)
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	l.Log(INFO, format, args...)
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	l.Log(ERROR, format, args...)
}
