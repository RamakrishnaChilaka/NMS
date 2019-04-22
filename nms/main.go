package main

import (
	"context"
	"nference/utils"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.etcd.io/etcd/client"
)

type configuration struct {
	etcdAddress   string
	etcdPathWatch string
	kafkaGroups   string
	KafkaBroker   string
}

var config configuration

func getEtcdClient() (kapi client.KeysAPI) {
	cfg := client.Config{
		Endpoints:               []string{config.etcdAddress},
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		utils.Danger(err, "Cannot connect to etcd client")
	}
	kapi = client.NewKeysAPI(c)
	return
}

func subscribeToTopics(topics []string, done chan struct{}) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config.KafkaBroker,
		"broker.address.family":           "v4",
		"group.id":                        config.kafkaGroups,
		"session.timeout.ms":              6000,
		"auto.offset.reset":               "earliest",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true})
	if err != nil {
		utils.Danger(err, "failed to create consumer")
	}
	defer c.Close()
	utils.P("created consumer")
	err = c.SubscribeTopics(topics, nil)
	run := true
	for run == true {
		select {
		case <-done:
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				// do something with this message
				utils.Info("Message partition is ", e.TopicPartition, " Message Value is ", e.Value)
			case kafka.PartitionEOF:
				utils.P("Reached ", e)
				return
			case kafka.Error:
				utils.Danger(e, "Kafka error!")
			}
		}
	}
}

func main() {
	configFile, err := os.Open("./config.json")
	if err != nil {
		utils.Danger(err, "Can't open configuration file")
	}
	utils.LoadConfig(&config,configFile)
	kapi := getEtcdClient()
	done := make(chan struct{})
	watcher := kapi.Watcher(config.etcdPathWatch, &client.WatcherOptions{
		Recursive: true,
	})

	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			utils.Danger(err, "Watcher event failed")
		}
		resp, err = kapi.Get(context.Background(), resp.Node.Key, nil)
		var topics []string
		for _, topic := range resp.Node.Nodes {
			topics = append(topics, topic.Value)
		}
		done <- struct{}{}
		for i := 0; i < 100; i++ {
			go subscribeToTopics(topics, done)
		}
	}
}
