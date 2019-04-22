package main

import (
	"net/http"
	"nference/utils"
	"os"
	"time"
)

type configuration struct {
	Address      string
	ReadTimeout  int64
	WriteTimeout int64
	kafkaBroker  string
}

var config configuration

func main() {
	file, err := os.Open("./config.json")
	if err != nil {
		utils.Danger(err, "Can't open configuration file")
	}
	utils.LoadConfig(&config, file)
	mux := http.NewServeMux()

	mux.HandleFunc("/start", topic)

	server := &http.Server{
		Addr:           config.Address,
		Handler:        mux,
		ReadTimeout:    time.Duration(config.ReadTimeout * int64(time.Second)),
		WriteTimeout:   time.Duration(config.WriteTimeout * int64(time.Second)),
		MaxHeaderBytes: 1 << 20,
	}
	server.ListenAndServe()
}
