package rmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Publisher defines the publisher for RabbitMQ
type Publisher struct {
	rmqUsername     string
	rmqPassword     string
	rmqHost         string
	rmqPort         string
	rmqExchange     string
	rmqExchangeType string
	connection      *amqp.Connection
	channel         *amqp.Channel
}

// NewPublisher constructs a RabbitMQ publisher
func NewPublisher(username string, password string, host string, port string, exchange string, exchangeType string) *Publisher {
	return &Publisher{
		rmqExchange:     exchange,
		rmqExchangeType: exchangeType,
		rmqHost:         host,
		rmqPassword:     password,
		rmqPort:         port,
		rmqUsername:     username,
	}
}

// Connect connects publisher
func (publisher *Publisher) Connect() error {
	rmqConnectionString := fmt.Sprintf("amqp://%s:%s@%s:%s/", publisher.rmqUsername, publisher.rmqPassword, publisher.rmqHost, publisher.rmqPort)
	conn, err := amqp.Dial(rmqConnectionString)
	if err != nil {
		log.Fatalf("[0] %s: %s", "Failed to connect to RabbitMQ", err)
		return nil
	}
	publisher.connection = conn

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[0] %s: %s", "Failed to open a channel in RabbitMQ", err)
		return nil
	}
	publisher.channel = ch

	err = ch.ExchangeDeclare(
		publisher.rmqExchange,     // name
		publisher.rmqExchangeType, // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("[0] %s: %s", "Failed to declare an exchange", err)
		return err
	}

	return nil
}

// Close closes connection to RabbitMQ
func (publisher *Publisher) Close() error {
	err := publisher.connection.Close()
	if err != nil {
		log.Fatalf("[0] %s: %s", "Failed to close connection", err)
		return err
	}
	return nil
}

// Publish publishes a message to RabbitMQ
func (publisher *Publisher) Publish(payload *Payload) error {
	if publisher.connection == nil {
		err := publisher.Connect()
		if err != nil {
			log.Printf(err.Error())
			return err
		}
	}
	err := publisher.channel.Publish(
		publisher.rmqExchange, // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: payload.ContentType,
			Body:        []byte(payload.Content),
		})

	if err != nil {
		log.Fatalf("[0] %s: %s", "Failed to publish payload", err)
		return err
	}
	return nil
}
