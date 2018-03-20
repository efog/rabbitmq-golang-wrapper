package rmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Subscriber receives publications from RabbitMQ
type Subscriber struct {
	handler     CommandHandler
	rmqUsername string
	rmqPassword string
	rmqHost     string
	rmqPort     string
	rmqExchange string
	connection  *amqp.Connection
	channel     *amqp.Channel
}

// CommandHandler command handler definition
type CommandHandler func(string) error

// NewSubscriber creates a new RabbitMQ subscriber
func NewSubscriber(username string, password string, host string, port string, exchange string, handler CommandHandler) *Subscriber {
	log.Printf("creating RMQ subscriber")
	return &Subscriber{
		handler:     handler,
		rmqExchange: exchange,
		rmqHost:     host,
		rmqPassword: password,
		rmqPort:     port,
		rmqUsername: username}
}

// Connect connects publisher
func (publisher *Subscriber) Connect() error {
	rmqConnectionString := fmt.Sprintf("amqp://%s:%s@%s:%s/", publisher.rmqUsername, publisher.rmqPassword, publisher.rmqHost, publisher.rmqPort)
	log.Printf("connecting to RabbitMQ using %s", rmqConnectionString)
	conn, err := amqp.Dial(rmqConnectionString)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
		return err
	}
	publisher.connection = conn
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Failed to open a channel in RabbitMQ", err)
		return err
	}
	publisher.channel = ch

	err = ch.ExchangeDeclare(
		publisher.rmqExchange, // name
		"fanout",              // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare an exchange", err)
		return err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare a queue", err)
		return err
	}

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		publisher.rmqExchange, // name
		false,
		nil)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to bind a queue", err)
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to consume channel", err)
		return err
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("[x] %s", d.Body)
			if publisher.handler != nil {
				publisher.handler(string(d.Body))
			}
		}
	}()

	log.Printf(" [*] Waiting for commands. To exit press CTRL+C")
	<-forever
	return nil
}
