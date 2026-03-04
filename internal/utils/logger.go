package utils

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	logMu   sync.Mutex
	verbose bool
)

// SetVerbose enables or disables verbose logging
func SetVerbose(v bool) {
	logMu.Lock()
	defer logMu.Unlock()
	verbose = v
}

// Logf logs a formatted message
func Logf(format string, v ...interface{}) {
	if verbose {
		log.Printf(format, v...)
	}
}

// LogAlways logs regardless of verbose setting
func LogAlways(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Fatal logs and exits
func Fatal(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)
	os.Exit(1)
}
