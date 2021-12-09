package amqp

import (
	"github.com/teris-io/shortid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
	"reflect"
	"os"
	"sync"
	"fmt"
)

var (
	conn    *amqp.Connection
	channel *amqp.Channel
	sid *shortid.Shortid;
	ConnectionIdentifier string
	channelMutex sync.Mutex

	AmqpHost string
	AmqpUser string
	AmqpPass string
	AmqpPort int
)

// A dumb Delivery wrapper, so dependencies on this lib don't have to depened on the streadway lib
type Delivery struct {
	Message amqp.Delivery
}

type HandlerFunc func(d Delivery)

func getDialURL() string {
	log.WithFields(log.Fields{
		"host": AmqpHost,
		"user": AmqpUser,
		"port": AmqpPort,
	}).Infof("AMQP Dial URL")

	return fmt.Sprintf("amqp://%v:%v@%v:%v", AmqpUser, AmqpPass, AmqpHost, AmqpPort)
}

func GetChannel() (*amqp.Channel, error) {
	var err error

	channelMutex.Lock()

	if channel == nil {
		log.Debugf("GetChannel() - Creating conn")

		sid, err = shortid.New(1, shortid.DefaultABC, 2342)

		if err != nil {
			return nil, err
		}

		cfg := amqp.Config{
			Properties: amqp.Table{
				"connection_name": ConnectionIdentifier + " on " + getHostname(),
			},
		}

		conn, err = amqp.DialConfig(getDialURL(), cfg)

		if err != nil {
			return nil, err
		}

		//defer conn.Close()

		if err != nil {
			log.Warnf("Could not get chan: %s", err)
		}

		channel, err = conn.Channel()
		log.Debugf("GetChannel() - Connected")
	}

	channelMutex.Unlock()

	return channel, err
}

func Publish(c *amqp.Channel, routingKey string, msg amqp.Publishing) error {
	err := c.Publish(
		"ex_upsilon",
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	return err
}

func getKey(msg interface{}) string {
    if t := reflect.TypeOf(msg); t.Kind() == reflect.Ptr {
        return t.Elem().Name()
    } else {
        return t.Name()
    }
}

func PublishPb(c *amqp.Channel, msg interface{}) {
	env := NewEnvelope(Encode(msg))

	msgKey := getKey(msg)

	log.Debugf("Publish: %+v", msgKey)

	Publish(c, msgKey, env)
}

func getHostname() string {
	hostname, err := os.Hostname()

	if err != nil {
		return "unknown"
	}

	return hostname
}

func Consume(c *amqp.Channel, deliveryTag string, handlerFunc HandlerFunc) error {
	id, _ := sid.Generate()

	queueName := getHostname() + "-" + id + "-" + deliveryTag

	_, err := c.QueueDeclare(
		queueName,
		false, // durable
		true, // delete when unused
		false, // exclusive
		true, // nowait
		nil, // args
	)

	if err != nil {
		return err
	}

	err = c.QueueBind(
		queueName, 
		deliveryTag, // key
		"ex_upsilon",
		true, // nowait
		nil, // args
	)

	if err != nil {
		return err
	}

	var done chan error;

	deliveries, err := c.Consume(
		queueName,  // name
		"consumer_tag",      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)

	if err != nil {
		return err
	}

	go consumeDeliveries(deliveries, done, handlerFunc)	

	for {
		time.Sleep(10 * time.Second)
	}

	return nil
}

func NewEnvelope(body []byte) amqp.Publishing {
	return amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp: time.Now(),
		ContentType: "application/binary",
		Body: body,
	}
}

func consumeDeliveries(deliveries <-chan amqp.Delivery, done chan error, handlerFunc HandlerFunc) {
	for d := range deliveries {
		handlerFunc(Delivery {
			Message: d,
		})
	}

	log.Infof("handle: deliveries channel closed")

	done <- nil
}

func StartServerListener() {
	log.Info("Started listening")
}
