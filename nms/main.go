package main

import (
	"context"
	"nference/push"
	"nference/utils"
	"os"
	"time"

	"github.com/go-stomp/stomp"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.etcd.io/etcd/client"
)

type configuration struct {
	etcdAddress      string
	etcdPathWatch    string
	kafkaGroups      string
	KafkaBroker      string
	rabbitMqAddress  string
	rabbitMqUserName string
	rabbitMqPassword string
}

var config configuration
var rabbitMqConn *stomp.Conn
var rabbitMqSub *stomp.Subscription
var clients = make(map[string]string)

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

func subscribeToTopics(ctx context.Context, topics []string) {
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
		return
	}
	defer c.Close()
	utils.P("created consumer")
	err = c.SubscribeTopics(topics, nil)
	run := true
	for run == true {
		select {
		case <-ctx.Done():
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				// do something with this message
				// unparse it and extract ClientId, either through kafka headers
				utils.Info("Message partition is ", e.TopicPartition, " Message Value is ", e.Value)
				clientID := "123" // hardCoded for now
				push.RabbitMQ(ctx, rabbitMqConn, e.Value, clients[clientID])

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
	utils.LoadConfig(&config, configFile)
	kapi := getEtcdClient()
	running := 0 // flag
	watcher := kapi.Watcher(config.etcdPathWatch, &client.WatcherOptions{
		Recursive: true,
	})
	// RabbitMqConnection
	rabbitMqConn, rabbitMqSub, _ = push.GetRabbitMqConnection(config.rabbitMqAddress, config.rabbitMqUserName, config.rabbitMqPassword)
	go func() {
		for {
			tempMessage := <-rabbitMqSub.C
			replyToHeader := tempMessage.Header.Get("reply-to")
			clientID := tempMessage.Header.Get("clientId")
			clients[clientID] = replyToHeader
		}
	}()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	for {
		if running == 1 {
			_, err := watcher.Next(context.Background()) // blocking operation
			if err != nil {
				utils.Danger(err, "watch event failed")
				return
			}
			cancelFunc()
		}
		resp, err := kapi.Get(context.Background(), config.etcdPathWatch, nil)
		if err != nil {
			utils.Danger(err, "Can't get from etcd")
			return
		}
		var topics []string
		for _, topic := range resp.Node.Nodes {
			topics = append(topics, topic.Value)
		}
		for i := 0; i < 100; i++ {
			go subscribeToTopics(ctx, topics)
		}
		running = 1
	}
}
