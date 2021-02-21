// From sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

var amqpURI = "amqp://guest:guest@localhost:5672"

func main() {
	time.Sleep(30 * time.Second)
	amqpConfig := amqp.NewNonDurablePubSubConfig(amqpURI, amqp.GenerateQueueNameTopicNameWithSuffix("q1"))

	subscriber, err := amqp.NewSubscriber(
		// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-two-go.html
		// It works as a simple queue.
		//
		// If you want to implement a Pub/Sub style service instead, check
		// https://watermill.io/pubsubs/amqp/#amqp-consumer-groups
		amqpConfig,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	amqpConfig2 := amqp.NewNonDurablePubSubConfig(amqpURI, amqp.GenerateQueueNameTopicNameWithSuffix("q2"))

	subscriber2, err := amqp.NewSubscriber(
		// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-two-go.html
		// It works as a simple queue.
		//
		// If you want to implement a Pub/Sub style service instead, check
		// https://watermill.io/pubsubs/amqp/#amqp-consumer-groups
		amqpConfig2,
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example.topic1")
	if err != nil {
		panic(err)
	}

	messages2, err := subscriber2.Subscribe(context.Background(), "example.topic1")
	if err != nil {
		panic(err)
	}

	go newProcess("sub1")(messages)
	go newProcess("sub2")(messages2)

	amqpConfig3 := amqp.NewNonDurablePubSubConfig(amqpURI, amqp.GenerateQueueNameTopicName)
	publisher, err := amqp.NewPublisher(amqpConfig3, watermill.NewStdLogger(false, false))
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!!! i am from q1"))

		if err := publisher.Publish("example.topic1", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func newProcess(id string) func(messages <-chan *message.Message) {
	return func(messages <-chan *message.Message) {
		for msg := range messages {
			log.Printf("%s received message: %s, payload: %s", id, msg.UUID, string(msg.Payload))

			// we need to Acknowledge that we received and processed the message,
			// otherwise, it will be resent over and over again.
			msg.Ack()
		}
	}
}
