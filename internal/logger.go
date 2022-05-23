package internal

import (
	"log"
	"os"
	"strings"
)

type Logger struct {
	debug   bool
	loggers map[string]*log.Logger
}

func NewLogger(debug bool) *Logger {
	l := &Logger{debug: debug, loggers: map[string]*log.Logger{}}
	for _, lvl := range []string{"info ", "error", "debug"} {
		l.loggers[strings.TrimSpace(lvl)] = log.New(os.Stderr, "["+strings.ToUpper(lvl)+"] ", log.LstdFlags)
	}
	return l
}

func (l *Logger) log(lvl, fmt string, args ...interface{}) {
	if l == nil {
		return
	}
	log := l.loggers[lvl]
	log.Printf(fmt, args...)
}

func (l *Logger) Info(fmt string, args ...interface{})  { l.log("info", fmt, args...) }
func (l *Logger) Error(fmt string, args ...interface{}) { l.log("error", fmt, args...) }
func (l *Logger) Debug(fmt string, args ...interface{}) {
	if l != nil && l.debug {
		l.log("debug", fmt, args...)
	}
}
