package consumer

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	die       chan struct{}
	conn      *amqp.Connection
	channel   *amqp.Channel
	QueueName string //队列名称
	Exchange  string //交换机
	Key       string //Key
	Mqurl     string //连接信息
}

func newRabbitMq() IRabbitMQ {
	return &RabbitMQ{
		die: make(chan struct{}),
	}
}

func (r *RabbitMQ) Start() error {
	fmt.Println(111)
	r.NewRabbitMQCli("at", "ex.direct", "at", "amqp://root:123456@192.168.0.104:5672/")
	r.ConsumeSimple()
	<-r.die
	return nil
}

func (r *RabbitMQ) Stop() {
	close(r.die)
}

//创建RabbitMQ结构体实例
func (r *RabbitMQ) NewRabbitMQCli(queueName string, exchange string, key string, mqurl string) {
	r.QueueName = queueName
	r.Exchange = exchange
	r.Key = key
	r.Mqurl = mqurl

	var err error
	//创建rabbitmq连接
	r.conn, err = amqp.Dial(r.Mqurl)
	r.failOnErr(err, "创建连接错误")
	r.channel, err = r.conn.Channel()
	r.failOnErr(err, "获取Channel失败")
	return
}

//断开channel和connection
func (r *RabbitMQ) Destoryy() {
	r.channel.Close()
	r.conn.Close()
}

//错误处理函数
func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Error().Msgf("%s:%s\n", message, err)
	}
}

//简单模式Step：2、简单模式下生产代码
func (r *RabbitMQ) PublishSimple(message string) {
	//1、申请消息队列，如果队列不存在会自动创建，如果存在则跳过创建
	//好处：宝成队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//是否持久化
		true,
		//是否为自动删除
		false,
		//是否具有排他性
		false,
		//是否阻塞
		false,
		//额外属性
		nil,
	)
	r.failOnErr(err, "publish")

	//2、发送消息到队列中
	err = r.channel.Publish(
		r.Exchange,
		r.QueueName,
		//如果为true，根据exchange类型和routkey规则，如果无法找到符合条件的队列，那么会把发送的消息返回给发送者
		true,
		//如果为true，当exchange发送消息到队列侯发现队列上没有绑定消费者，则会把消息发还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		log.Error().Msgf("%s:%s\n", "publish", err)
	}
	log.Debug().Msgf("publish:%s\n", message)
}

//简单模式Step：3、消费
func (r *RabbitMQ) ConsumeSimple() {
	//1、申请队列
	_, err := r.channel.QueueDeclare(r.QueueName, true, false, false, false, nil)
	r.failOnErr(err, "consume queue declare")
	//2、接收消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		//用来区分多个消费者
		"",
		//是否自动应答
		true,
		//是否具有排他性
		false,
		//如果设置为true，表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//消息队列是否阻塞
		false,
		nil,
	)
	r.failOnErr(err, "consume")

	forever := make(chan bool)

	//3、启动协程处理消息
	go func() {
		for d := range msgs {
			//实现我们要处理的逻辑函数
			log.Printf("Received a message : %s", d.Body)
		}
	}()

	log.Printf("[*] Waiting for messagees,To exit press CTRL+C")

	<-forever
}
