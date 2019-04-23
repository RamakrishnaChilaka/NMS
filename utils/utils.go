package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

var logger *log.Logger

// P Convenience function for printing to stdout
func P(a ...interface{}) {
	fmt.Println(a...)
}

// LoadConfig loads configuration into config struct
func LoadConfig(config interface{}, file *os.File) {
	decoder := json.NewDecoder(file)
	err := decoder.Decode(config)
	if err != nil {
		log.Fatalln("Cannot get configuration from file", err)
	}
}

// Info for logging
func Info(args ...interface{}) {
	logger.SetPrefix("INFO ")
	logger.Println(args...)
}

// Danger logging
func Danger(args ...interface{}) {
	logger.SetPrefix("ERROR ")
	logger.Println(args...)
}

// Warning logging
func Warning(args ...interface{}) {
	logger.SetPrefix("WARNING ")
	logger.Println(args...)
}

// version
func version() string {
	return "0.1"
}
