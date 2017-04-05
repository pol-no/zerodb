package frontend

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/vitalvas/zerodb/db"

	"github.com/streadway/amqp"
)

func amqpService(wg *sync.WaitGroup) {
	defer wg.Done()

	connect := os.Getenv("ZERODB_AMQP_CONNECT")
	subscribe := os.Getenv("ZERODB_AMQP_SUBSCRIBE")
	channels := os.Getenv("ZERODB_AMQP_CHANNELS")

	if len(connect) < 24 || len(subscribe) < 4 {
		return
	}

	log.Println("AMQP subscribe on", subscribe)

	channel := db.API.GetDBCount()

	if len(channels) > 0 {
		var err error
		channel, err = strconv.Atoi(channels)
		if err != nil {
			return
		}
	}

	conn, err := amqp.Dial(connect)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	forever := make(chan bool)

	for i := 0; i < channel; i++ {
		go func(conn *amqp.Connection, chanid int) {
			ch, err := conn.Channel()
			failOnError(err, "Failed to open a channel")
			defer ch.Close()

			err = ch.Qos(1, 0, false)
			failOnError(err, "Failed to set QoS")

			msgs, err := ch.Consume(subscribe, fmt.Sprintf("zerodb-%d", chanid), false, false, false, false, nil)
			failOnError(err, "Failed to register a consumer")

			for msg := range msgs {
				method := "Pass"

				if msg.Headers["Method"] != nil {
					method = msg.Headers["Method"].(string)
				}

				if msg.Headers["Key"] == nil {
					amqpSend(msg, ch, []byte(fmt.Sprint("Required param 'Key'")))
				}

				key := []byte(msg.Headers["Key"].(string))

				switch method {
				case "Read":
					rawdata, err := db.API.Read(key)
					failOnError(err, "Failed to read a message from DB")
					amqpSend(msg, ch, rawdata)

				case "Write":
					err := db.API.Write(key, msg.Body)
					failOnError(err, "Failed to write a message from DB")
					amqpSend(msg, ch, []byte("OK"))

				case "Delete":
					err := db.API.Delete(key)
					failOnError(err, "Failed to delete a message from DB")
					amqpSend(msg, ch, []byte("OK"))

				case "Exists":
					exists, err := db.API.Exists(key)
					failOnError(err, "Failed to check exists a message from DB")
					amqpSend(msg, ch, []byte(btostr(exists)))

				default:
					amqpSend(msg, ch, []byte(fmt.Sprintf("Method '%s' not supported", method)))
				}

				msg.Ack(false)
			}
		}(conn, i)
	}

	<-forever
}

func amqpSend(msg amqp.Delivery, ch *amqp.Channel, body []byte) {
	pub := amqp.Publishing{
		AppId:         "ZeroDB",
		CorrelationId: msg.CorrelationId,
		Timestamp:     time.Now(),
		Body:          body,
	}
	err := ch.Publish(msg.ContentType, msg.ReplyTo, false, false, pub)
	failOnError(err, "Failed to publish a message")
}
