/**
 * <p>Description: (一句话描述一下该文件的作用) </>
 * @author lizhi_duan
 * @date 2019/4/12 21:25
 * @version 1.0
 */
package rabbitmq_go

import "time"

const (
	DirectExchange  = "direct"  //处理路由键,把消息路由到那些binding key与routing key完全匹配的Queue中。
	FanoutExchange  = "fanout"  //不处理路由键,把所有发送到该Exchange的消息路由到所有与它绑定的Queue中。
	TopicExchange   = "topic"   //将路由键和某模式进行匹配。此时队列需要绑定要一个模式上。符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。
	HeadersExchange = "headers" //不依赖于routing key与binding key的匹配规则来路由消息，而是根据发送的消息内容中的headers属性进行匹配
)
const channelMax = 10

type IConfig struct {
	vhost      string //虚拟主机
	channelMax int    // 0 max channels means 2^16 - 1
	properties map[string]interface{}
}

//You can set other message properties here
type Properties struct {
	ContentType     string        //消息内容的类型 content_type
	ContentEncoding string        //消息内容的编码格式 content_encoding
	Priority        string        //消息优先级
	MessageId       string        //消息id message_id
	Timestamp       time.Time     //消息的时间戳timestamp
	UserId          string        //用户id user_id
	Expiration      time.Duration //expiration消息的失效时间
}

func NewIConfig() IConfig {
	var conf IConfig
	conf.vhost = "/"
	conf.channelMax = channelMax
	return conf
}
func NewIConfigByVHost(vhost string) IConfig {
	var conf IConfig
	if vhost == "" {
		conf.vhost = "/"
	} else {
		conf.vhost = vhost
	}
	return conf
}

func NewIConfigByHostAndMaxChannel(vhost string, maxChannel int) IConfig {
	conf := NewIConfigByVHost(vhost)
	if maxChannel <= 0 {
		maxChannel = channelMax
	}
	conf.channelMax = maxChannel
	return conf
}

//create conf
func NewIConfigAll(vhost string, maxChannel int, properties Properties) IConfig {
	conf := NewIConfigByHostAndMaxChannel(vhost, maxChannel)
	var confMap map[string]interface{}
	confMap["content_type"] = properties.ContentType
	confMap["content_encoding"] = properties.ContentEncoding
	confMap["priority"] = properties.Priority
	confMap["message_id"] = properties.MessageId
	confMap["timestamp"] = properties.Timestamp
	confMap["user_id"] = properties.UserId
	confMap["expiration"] = properties.Expiration
	conf.properties = confMap
	return conf
}

//获取config
func (ic IConfig) GetConfig() (VHost string, maxChannel int, properties map[string]interface{}) {
	return ic.vhost, ic.channelMax, ic.properties
}
