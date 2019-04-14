package producer

import (
	"fmt"
	"log"
	rabbitmq_go "rabbitmq-go"
	"time"

	"github.com/duanlizhi/go-utils-lizhi/transform"

	"github.com/streadway/amqp"
)

//生产者interface
type IProducer interface {
	Publish(body []byte, exchangeName string, exchangeType string, routingKey string,
		confirmCallback func(confirms <-chan amqp.Confirmation)) error
	PublishWithConfig(body []byte, exchangeName string, exchangeType string, routingKey string, config rabbitmq_go.IConfig,
		confirmCallback func(confirms <-chan amqp.Confirmation)) error
	PublishOnQueue(body []byte, queueName string,
		confirmCallback func(confirms <-chan amqp.Confirmation)) error
	PublishOnQueueWithConfig(body []byte, queueName string, config rabbitmq_go.IConfig,
		confirmCallback func(confirms <-chan amqp.Confirmation)) error
}

//生产者interface implement
type Producer struct {
	conn                *amqp.Connection
	durable, autoDelete bool
}

//创建消费者，默认持久化到硬盘、自动删除无用队列
func NewProducer(amqpURI string) *Producer {
	return NewProducerByDurableAndAutoDelete(amqpURI, true, true)
}

//创建消费者
func NewProducerByDurableAndAutoDelete(amqpURI string, durable, autoDelete bool) *Producer {
	if amqpURI == "" {
		panic("Cannot initialize connection to broker, please check your profile. Have you initialized?")
	}

	var err error
	var producer Producer
	producer.conn, err = amqp.Dial(fmt.Sprintf("%s/", amqpURI))
	if err != nil {
		panic("Failed to connect to AMQP compatible broker at: " + amqpURI)
	}
	producer.durable = durable
	producer.autoDelete = autoDelete
	return &producer
}

//创建消费者，默认持久化到硬盘，自动删除无连接队列
func NewProducerByConfig(amqpURI string, config rabbitmq_go.IConfig) *Producer {
	return NewProducerByConfigDurableAndDelete(amqpURI, true, true, config)
}

//init producer by config,durable,autoDelete
func NewProducerByConfigDurableAndDelete(amqpURI string, durable, autoDelete bool, config rabbitmq_go.IConfig) *Producer {
	if amqpURI == "" {
		panic("Cannot initialize connection to broker, please check your profile. Have you initialized?")
	}
	var err error
	var producer Producer
	var conf amqp.Config
	conf.Vhost, conf.ChannelMax, conf.Properties = config.GetConfig()

	producer.conn, err = amqp.DialConfig(fmt.Sprintf("%s/", amqpURI), conf)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to AMQP compatible broker at: %s\n error msg: %s", amqpURI, err))
	}
	producer.durable = durable
	producer.autoDelete = autoDelete
	return &producer
}

//发布消息
//如果需要手动确认发布成功，那么请传入确认回调函数，不需要的话传入nil
func (p *Producer) Publish(body []byte, exchangeName string, exchangeType string, routingKey string,
	confirmCallback func(confirms <-chan amqp.Confirmation)) error {
	if exchangeType == "" {
		exchangeType = rabbitmq_go.TopicExchange
	}
	if routingKey == "" {
		routingKey = exchangeName
	}
	if p.conn == nil {
		return fmt.Errorf("the connection not initialized")
	}
	ch, err := p.conn.Channel() // Get a channel from the connection
	defer ch.Close()
	err = ch.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		p.durable,    // durable
		p.autoDelete, // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("ExchangeDeclare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if confirmCallback != nil {
		log.Printf("enabling publishing confirms.")
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmCallback(confirms)
	}

	queue, err := ch.QueueDeclare( // Declare a queue that will be created if not exists with some args
		exchangeName, // our queue name
		p.durable,    // durable
		p.autoDelete, // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	err = ch.QueueBind(
		queue.Name,   // name of the queue
		routingKey,   // bindingKey
		exchangeName, // sourceExchange
		false,        // noWait
		nil,          // arguments
	)
	err = ch.Publish( // Publishes a message onto the queue.
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body: body, // Our JSON body as []byte
		})
	fmt.Printf("A message was sent: %v\n", body)
	return err
}
func (p *Producer) PublishWithConfig(body []byte, exchangeName string, exchangeType string, routingKey string, config rabbitmq_go.IConfig,
	confirmCallback func(confirms <-chan amqp.Confirmation)) error {
	if exchangeType == "" {
		exchangeType = rabbitmq_go.TopicExchange
	}
	if routingKey == "" {
		routingKey = exchangeName
	}
	if p.conn == nil {
		return fmt.Errorf("the connection not initialized")
	}
	ch, err := p.conn.Channel() // Get a channel from the connection
	defer ch.Close()
	err = ch.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		p.durable,    // durable
		p.autoDelete, // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("ExchangeDeclare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if confirmCallback != nil {
		log.Printf("enabling publishing confirms.")
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmCallback(confirms)
	}

	queue, err := ch.QueueDeclare( // Declare a queue that will be created if not exists with some args
		exchangeName, // our queue name
		p.durable,    // durable
		p.autoDelete, // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	err = ch.QueueBind(
		queue.Name,   // name of the queue
		routingKey,   // bindingKey
		exchangeName, // sourceExchange
		false,        // noWait
		nil,          // arguments
	)
	_, _, properties := config.GetConfig()
	i := properties["Timestamp"]
	i2 := i.(time.Time)
	err = ch.Publish( // Publishes a message onto the queue.
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body:            body, // Our JSON body as []byte
			ContentType:     transform.BaseType2String(properties["ContentType"]),
			ContentEncoding: transform.BaseType2String(properties["ContentEncoding"]),
			DeliveryMode:    uint8(transform.String2Int(transform.BaseType2String(properties["DeliveryMode"]))),
			Priority:        uint8(transform.String2Int(transform.BaseType2String(properties["Priority"]))),
			CorrelationId:   transform.BaseType2String(properties["CorrelationId"]),
			ReplyTo:         transform.BaseType2String(properties["ReplyTo"]),
			Expiration:      transform.BaseType2String("Expiration"),
			MessageId:       transform.BaseType2String("MessageId"),
			Timestamp:       i2,
			Type:            transform.BaseType2String("Type"),
			UserId:          transform.BaseType2String("UserId"),
			AppId:           transform.BaseType2String("AppId"),
		})
	fmt.Printf("A message was sent: %v\n", body)
	return err
}

//发布到指定的队列
//
func (p *Producer) PublishOnQueue(body []byte, queueName string, confirmCallback func(confirms <-chan amqp.Confirmation)) error {

	if p.conn == nil {
		panic("Tried to send message before connection was initialized. Don't do that.")
	}
	ch, err := p.conn.Channel() // Get a channel from the connection
	defer ch.Close()

	queue, err := ch.QueueDeclare( // Declare a queue that will be created if not exists with some args
		queueName, // our queue name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if confirmCallback != nil {
		log.Printf("enabling publishing confirms.")
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmCallback(confirms)
	}
	// Publishes a message onto the queue.
	err = ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body, // Our JSON body as []byte
		})
	fmt.Printf("A message was sent to queue %v: %v\n", queueName, body)
	return err
}

func (p *Producer) PublishOnQueueWithConfig(body []byte, queueName string, config rabbitmq_go.IConfig,
	confirmCallback func(confirms <-chan amqp.Confirmation)) error {
	if p.conn == nil {
		panic("Tried to send message before connection was initialized. Don't do that.")
	}
	ch, err := p.conn.Channel() // Get a channel from the connection
	defer ch.Close()

	queue, err := ch.QueueDeclare( // Declare a queue that will be created if not exists with some args
		queueName, // our queue name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if confirmCallback != nil {
		log.Printf("enabling publishing confirms.")
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmCallback(confirms)
	}
	_, _, properties := config.GetConfig()
	i := properties["Timestamp"]
	i2 := i.(time.Time)
	// Publishes a message onto the queue.
	err = ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body:            body, // Our JSON body as []byte
			ContentType:     transform.BaseType2String(properties["ContentType"]),
			ContentEncoding: transform.BaseType2String(properties["ContentEncoding"]),
			DeliveryMode:    uint8(transform.String2Int(transform.BaseType2String(properties["DeliveryMode"]))),
			Priority:        uint8(transform.String2Int(transform.BaseType2String(properties["Priority"]))),
			CorrelationId:   transform.BaseType2String(properties["CorrelationId"]),
			ReplyTo:         transform.BaseType2String(properties["ReplyTo"]),
			Expiration:      transform.BaseType2String("Expiration"),
			MessageId:       transform.BaseType2String("MessageId"),
			Timestamp:       i2,
			Type:            transform.BaseType2String("Type"),
			UserId:          transform.BaseType2String("UserId"),
			AppId:           transform.BaseType2String("AppId"),
		})
	fmt.Printf("A message was sent to queue %v: %v\n", queueName, body)
	return err
}

//confirm确认回调函数
func ConfirmCallback(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
