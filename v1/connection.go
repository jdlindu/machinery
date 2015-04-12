package v1

import (
	"github.com/streadway/amqp"
)

// Connection dials a broker and opens a channel for communication
type Connection struct {
	config  *Config
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queue   amqp.Queue
}

// InitConnection - Connection constructor
func InitConnection(config *Config) *Connection {
	return &Connection{
		config: config,
	}
}

// Open connects to the message queue, opens a channel,
// declares a queue and returns connection, channel
// and queue objects
func (c *Connection) Open() *Connection {
	var err error

	c.Conn, err = amqp.Dial(c.config.BrokerURL)
	FailOnError(err, "Failed to connect to RabbitMQ")

	c.Channel, err = c.Conn.Channel()
	FailOnError(err, "Failed to open a channel")

	c.Queue, err = c.Channel.QueueDeclare(
		c.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	return c
}

// Close shuts down the connection
func (c *Connection) Close() {
	c.Conn.Close()
	c.Channel.Close()
}

// PublishMessage places a new message on the default queue
func (c *Connection) PublishMessage(body []byte) {
	err := c.Channel.Publish(
		"",           // exchange
		c.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	FailOnError(err, "Failed to publish a message")
}