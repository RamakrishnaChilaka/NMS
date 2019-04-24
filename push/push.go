package push

import (
	"context"
	"log"

	"github.com/go-stomp/stomp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// GetRabbitMqConnection returns new RabbitMq Connection
func GetRabbitMqConnection(addr, username, password string) (conn *stomp.Conn, sub *stomp.Subscription, err error) {
	conn, err = stomp.Dial("tcp", addr,
		stomp.ConnOpt.Login(username, password),
		stomp.ConnOpt.Host("/"))
	sub, err = conn.Subscribe("/queue/test", stomp.AckAuto)
	failOnError(err, "Subscription to test failed")
	return
}

// RabbitMQ exported function
func RabbitMQ(ctx context.Context, conn *stomp.Conn, message []byte, destination string) {
	conn.Send(destination, "text/plain", []byte(message))
}
