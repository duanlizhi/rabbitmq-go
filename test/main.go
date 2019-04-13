package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	rabbitmq_go "rabbitmq-go"
	"rabbitmq-go/consumer"

	"github.com/streadway/amqp"
)

const AmqpURI = "amqp://xcj:xcj@localhost:5672"

func main() {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {

	})
	iConsumer := consumer.NewIConsumer(AmqpURI)
	iConsumer.Subscribe("test1", rabbitmq_go.TopicExchange, "test1", true, Handler)
	err := http.ListenAndServe(":7777", nil)
	if err != nil {
		panic(err)
	}
}

type Message struct {
	Id   string
	Text string
}

func Handler(d amqp.Delivery) {
	log.Println("接收到消息。。。")
	body := d.Body
	consumerTag := d.ConsumerTag
	var msg Message
	json.Unmarshal(body, &msg)
	fmt.Println(msg)
	fmt.Println(consumerTag)
}
