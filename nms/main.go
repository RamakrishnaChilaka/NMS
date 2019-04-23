package main

import (
	"context"
	"nference/push"
	"nference/utils"
	"time"

	"github.com/go-stomp/stomp"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.etcd.io/etcd/client"
)

type configuration struct {
	etcdAddress      string
	etcdPathWatch    string
	kafkaGroups      string
	kafkaBroker      string
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
		"bootstrap.servers":               config.kafkaBroker,
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
			utils.P("context got cancelled")
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				// do something with this message
				// unparse it and extract ClientId, either through kafka headers
				utils.P("Message partition is ", e.TopicPartition, " Message Value is ", string(e.Value))
				clientID := "123" // hardCoded for now
				utils.P(clients)
				if clientQ, queueExists := clients[clientID]; queueExists {
					utils.P("client is connected, since tempq exists")
					push.RabbitMQ(ctx, rabbitMqConn, e.Value, clientQ)
				} else {
					// this means that client is not connected
					// do some default behaviour here to ensure gauranteed delivery
				}
			case kafka.PartitionEOF:
				utils.P("Reached ", e)
			case kafka.Error:
				utils.Danger(e, "Kafka error!")
			}
		}
	}
}

func main() {

	config = configuration{
		etcdAddress:      "http://127.0.0.1:2379",
		etcdPathWatch:    "/topics",
		kafkaGroups:      "kafkaGID",
		kafkaBroker:      "localhost",
		rabbitMqAddress:  "localhost:61613",
		rabbitMqUserName: "guest",
		rabbitMqPassword: "guest",
	}
	utils.P(config)
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
			utils.P("Recieved message in queue subscriptions", tempMessage)
			replyToHeader := tempMessage.Header.Get("reply-to")
			clientID := tempMessage.Header.Get("clientID")
			clients[clientID] = replyToHeader
		}
	}()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	for {
		if running == 1 {
			utils.P("waiting for watcher event")
			_, err := watcher.Next(context.Background()) // blocking operation
			if err != nil {
				utils.Danger(err, "watch event failed")
			}
			cancelFunc()
		}
		resp, err := kapi.Get(context.Background(), config.etcdPathWatch, nil)
		utils.P(resp.Node.Nodes, " these are Nodes")
		if err != nil {
			utils.Danger(err, "Can't get from etcd")
			return
		}
		var topics []string
		for _, topic := range resp.Node.Nodes {
			topics = append(topics, topic.Key[8:])
		}
		utils.P(topics, " These are topics")
		for i := 0; i < 5; i++ {
			go subscribeToTopics(ctx, topics)
		}
		running = 1
	}
}
