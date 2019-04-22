package push

import (
	"log"

	"github.com/go-stomp/stomp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// PushToRabbitMQ exported function
func PushToRabbitMQ(strMsg string) {
	conn, err := stomp.Dial("tcp", "localhost:61613",
		stomp.ConnOpt.Login("guest", "guest"),
		stomp.ConnOpt.Host("/"))
	failOnError(err, "Stomp dailing failed")
	sub, err := conn.Subscribe("/queue/test", stomp.AckClient)
	failOnError(err, "Subscription to test failed")
	for {
		msg := <-sub.C
		log.Printf("Recieved a message: %s", msg.Body)
		replyToHeader := msg.Header.Get("reply-to")
		log.Printf("Recieved a header: %s", replyToHeader)
		conn.Send(replyToHeader, "text/plain", []byte(strMsg))
		//conn.Send(replyToHeader, "text/plain", msg.Body)
		err = conn.Ack(msg)
		failOnError(err, "Failed to acknowledge the message")
	}
}
