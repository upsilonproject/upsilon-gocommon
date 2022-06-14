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
	channels map[string]*amqp.Channel
	ConnectionIdentifier string
	connMutex sync.Mutex

	InstanceId string

	AmqpHost string
	AmqpUser string
	AmqpPass string
	AmqpPort int
)

func init() {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)

	if err != nil {
		log.Fatalf("Could not generate AMQP shortid %v", err)
	}

	InstanceId, _ = sid.Generate()
}

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
		"instance": InstanceId,
	}).Infof("AMQP Dial URL")

	return fmt.Sprintf("amqp://%v:%v@%v:%v", AmqpUser, AmqpPass, AmqpHost, AmqpPort)
}

func GetChannel(name string) (*amqp.Channel, error) {
	conn, err := getConn()

	if err != nil {
		return nil, err
	}

	if channel, ok := channels[name]; !ok {
		log.Debugf("GetChannel() - Opening new channel for %v", name)

		channel, err = conn.Channel()

		if err != nil {
			log.Warnf("GetChannel() - Error opening channel: %v")
			return nil, err
		}

		channels[name] = channel
	}

	return channels[name], nil
}

func getConn() (*amqp.Connection, error) {
	var err error
	connMutex.Lock()

	if conn == nil || conn.IsClosed() {
		log.Debugf("getConn() - Creating conn")

		cfg := amqp.Config{
			Properties: amqp.Table{
				"connection_name": ConnectionIdentifier + " on " + getHostname(),
			},
		}

		conn, err = amqp.DialConfig(getDialURL(), cfg)

		if err != nil {
			log.Warnf("getConn() - Could not connect: %s", err)
			connMutex.Unlock()

			return nil, err
		}

		channels = make(map[string]*amqp.Channel)
	}

	connMutex.Unlock()

	return conn, nil
}

func getKey(msg interface{}) string {
    if t := reflect.TypeOf(msg); t.Kind() == reflect.Ptr {
        return t.Elem().Name()
    } else {
        return t.Name()
    }
}

func Publish(routingKey string, msg amqp.Publishing) error {
	channel, err := GetChannel("Publish-" + routingKey)

	if err != nil {
		log.Errorf("Publish error: %v", err)
		return err
	} else {
		return PublishWithChannel(channel, routingKey, msg)
	}
}

func PublishWithChannel(c *amqp.Channel, routingKey string, msg amqp.Publishing) error {
	err := c.Publish(
		"ex_upsilon",
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	return err
}

func PublishPb(msg interface{}) {
	channel, err := GetChannel("Publish-" + getKey(msg))

	if err != nil {
		log.Errorf("PublishPb: %v", err)
		return
	}

	PublishPbWithChannel(channel, msg)
}

func PublishPbWithChannel(c *amqp.Channel, msg interface{}) {
	env := NewEnvelope(Encode(msg))

	msgKey := getKey(msg)

	log.Debugf("PublishPbWithChannel: %+v", msgKey)

	Publish(msgKey, env)
}

func getHostname() string {
	hostname, err := os.Hostname()

	if err != nil {
		return "unknown"
	}

	return hostname
}

func Consume(deliveryTag string, handlerFunc HandlerFunc) {
	for { 
		channel, err := GetChannel(deliveryTag)

		if err != nil {
			log.Errorf("Get Channel error: %v", err)
		} else {
			consumeWithChannel(channel, deliveryTag, handlerFunc)
		}
		
		time.Sleep(10 * time.Second)
	}
}

func consumeWithChannel(c *amqp.Channel, deliveryTag string, handlerFunc HandlerFunc) {
	queueName := getHostname() + "-" + InstanceId + "-" + deliveryTag

	_, err := c.QueueDeclare(
		queueName,
		false, // durable
		true, // delete when unused
		false, // exclusive
		true, // nowait
		nil, // args
	)

	if err != nil {
		log.Warnf("Queue declare error: %v", err)
		return
	}

	err = c.QueueBind(
		queueName, 
		deliveryTag, // key
		"ex_upsilon",
		true, // nowait
		nil, // args
	)

	if err != nil {
		log.Warnf("Queue bind error: %v %v", deliveryTag, err)
		return 
	}

	log.Infof("Consumer channel creating for: %v", deliveryTag)

	deliveries, err := c.Consume(
		queueName,  // name
		"consume-" + deliveryTag, // consumer tag
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)

	if err != nil {
		log.Warnf("Consumer channel creation error: %v %v", deliveryTag, err)
		return
	}

	consumeDeliveries(deliveries, handlerFunc)	

	log.Infof("Consumer channel closed for: %v", deliveryTag)
}

func NewEnvelope(body []byte) amqp.Publishing {
	return amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp: time.Now(),
		ContentType: "application/binary",
		Body: body,
	}
}

func consumeDeliveries(deliveries <-chan amqp.Delivery, handlerFunc HandlerFunc) {
	for d := range deliveries {
		handlerFunc(Delivery {
			Message: d,
		})
	}
}

func StartServerListener() {
	log.Info("Started listening")
}
