package utils

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	// LogLevelDebug is for detailed debugging information
	LogLevelDebug LogLevel = iota
	// LogLevelInfo is for general operational information
	LogLevelInfo
	// LogLevelWarn is for warning messages
	LogLevelWarn
	// LogLevelError is for error messages
	LogLevelError
	// LogLevelFatal is for fatal error messages
	LogLevelFatal
)

var logLevelNames = map[LogLevel]string{
	LogLevelDebug: "DEBUG",
	LogLevelInfo:  "INFO",
	LogLevelWarn:  "WARN",
	LogLevelError: "ERROR",
	LogLevelFatal: "FATAL",
}

// Logger is a simple logging interface
type Logger struct {
	level LogLevel
	log   *log.Logger
}

// NewLogger creates a new logger with the specified minimum log level
func NewLogger(level LogLevel) *Logger {
	return &Logger{
		level: level,
		log:   log.New(os.Stdout, "", log.LstdFlags),
	}
}

// SetLevel sets the minimum log level
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevelFromString returns the log level from a string
func GetLevelFromString(levelStr string) LogLevel {
	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		return LogLevelDebug
	case "INFO":
		return LogLevelInfo
	case "WARN":
		return LogLevelWarn
	case "ERROR":
		return LogLevelError
	case "FATAL":
		return LogLevelFatal
	default:
		return LogLevelInfo
	}
}

// log message with level
func (l *Logger) logf(level LogLevel, format string, v ...interface{}) {
	if level >= l.level {
		l.log.Printf("[%s] %s", logLevelNames[level], fmt.Sprintf(format, v...))
	}
}

// Debugf logs a debug message
func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logf(LogLevelDebug, format, v...)
}

// Infof logs an info message
func (l *Logger) Infof(format string, v ...interface{}) {
	l.logf(LogLevelInfo, format, v...)
}

// Warnf logs a warning message
func (l *Logger) Warnf(format string, v ...interface{}) {
	l.logf(LogLevelWarn, format, v...)
}

// Errorf logs an error message
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logf(LogLevelError, format, v...)
}

// Fatalf logs a fatal message and exits
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logf(LogLevelFatal, format, v...)
	os.Exit(1)
}
