package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)


var logger *log.Logger

// Convenience function for printing to stdout
func P(a ...interface{}) {
	fmt.Println(a...)
}


func LoadConfig(config interface{}, file *os.File) {
	decoder := json.NewDecoder(file)
	err := decoder.Decode(config)
	if err != nil {
		log.Fatalln("Cannot get configuration from file", err)
	}
}

// for logging
func Info(args ...interface{}) {
	logger.SetPrefix("INFO ")
	logger.Println(args...)
}

func Danger(args ...interface{}) {
	logger.SetPrefix("ERROR ")
	logger.Println(args...)
}

func Warning(args ...interface{}) {
	logger.SetPrefix("WARNING ")
	logger.Println(args...)
}

// version
func version() string {
	return "0.1"
}
