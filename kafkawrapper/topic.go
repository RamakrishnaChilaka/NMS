package main

import (
	"net/http"
	"nference/utils"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func produceToTopics(topic, msg string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.kafkaBroker})

	if err != nil {
		utils.Danger("Failed to create producer")
		return
	}

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(msg)}

	p.Close()

}

func topic(writer http.ResponseWriter, request *http.Request) {
	produceToTopics("mytopic", "This is my message "+time.Now().String())
}
