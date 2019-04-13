# rabbitmq-go
rabbitmq golang server client

##package producer是作为rabbitmq的生产者存在
调用NewProducer()实例化一个生产者连接，producer实现了四个接口：
为保证消息百分百到达，提供了confirmCallback，由用户根据业务逻辑自由定义
###正常发布消息，经过交换机 》消息队列
Publish(msg []byte, exchangeName string, exchangeType string,
		confirmCallback func(confirms <-chan amqp.Confirmation)) error  
PublishWithConfig(msg []byte, exchangeName string, exchangeType string, conf rabbitmq_go.IConfig,
    confirmCallback func(confirms <-chan amqp.Confirmation)) error  
###单独发布消息到消息队列中，不经过交换机 
PublishOnQueue(body []byte, queueName string,
    confirmCallback func(confirms <-chan amqp.Confirmation)) error  
PublishOnQueueWithConfig(body []byte, queueName string, conf rabbitmq_go.IConfig,
    confirmCallback func(confirms <-chan amqp.Confirmation)) error  
##package consumer是作为rabbitmq的消费者存在
调用NewIConsumer()等方法获取到一个客户端的连接
consumer实现了以下两个接口：  
###订阅消息
Subscribe(exchangeName string, exchangeType string, consumerTag string, handlerFunc func(amqp.Delivery)) error  
###订阅直接发布到消息队列中的消息
SubscribeToQueue(queueName string, consumerTag string, handlerFunc func(amqp.Delivery)) error  
##备注
如果需要设定VHost，请在rabbitmq后台分配好VHost和用户的相关权限