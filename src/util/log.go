package util

import (
	"fmt"
	"log"
	"os"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

var currentLogLevel = INFO

type Logger struct {
	*log.Logger
	level LogLevel
}

func NewLogger(level LogLevel) *Logger {
	return &Logger{
		Logger: log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile),
		level:  level,
	}
}

func (l *Logger) SetLevel(level LogLevel) {
	currentLogLevel = level
}

func (l *Logger) Debug(format string, v ...interface{}) {
	if l.level <= DEBUG && currentLogLevel <= DEBUG {
		l.Output(2, "DEBUG: "+fmt.Sprintf(format, v...))
	}
}

func (l *Logger) Info(format string, v ...interface{}) {
	if l.level <= INFO && currentLogLevel <= INFO {
		l.Output(2, "INFO: "+fmt.Sprintf(format, v...))
	}
}

func (l *Logger) Warn(format string, v ...interface{}) {
	if l.level <= WARN && currentLogLevel <= WARN {
		l.Output(2, "WARN: "+fmt.Sprintf(format, v...))
	}
}

func (l *Logger) Error(format string, v ...interface{}) {
	if l.level <= ERROR && currentLogLevel <= ERROR {
		l.Output(2, "ERROR: "+fmt.Sprintf(format, v...))
	}
}
