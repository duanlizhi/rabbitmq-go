/**
 * <p>Description: (一句话描述一下该文件的作用) </>
 * @author lizhi_duan
 * @date 2019/4/13 10:38
 * @version 1.0
 */
package producer

import (
	"encoding/json"
	"log"
	rabbitmq_go "rabbitmq-go"
	"testing"

	"github.com/streadway/amqp"
)

const AmqpURI = "amqp://xcj:xcj@localhost:5672"

func TestProducer_Publish(t *testing.T) {

	producer := NewProducer(AmqpURI)
	//producer := NewProducer(AmqpURI)
	var msg Message
	msg.Id = "1"
	msg.Text = "测试test1，看看rabbitmq如何"
	bytes, _ := json.Marshal(msg)
	producer.Publish(bytes, "test1", rabbitmq_go.TopicExchange, "", ConfirmCallback1)
}

type Message struct {
	Id   string
	Text string
}

//confirm确认回调函数
func ConfirmCallback1(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing 1")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
