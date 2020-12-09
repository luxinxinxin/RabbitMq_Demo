package publisher

import (
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	die       chan struct{}
	conn      *amqp.Connection
	channel   *amqp.Channel
	QueueName string //队列名称
	Exchange  string //交换器
	Key       string //Key
	Mqurl     string //连接信息
}

func newRabbitMq() IRabbitMQ {
	return &RabbitMQ{
		die: make(chan struct{}),
	}
}

func (r *RabbitMQ) Start() error {
	err := r.NewRabbitMQCli("at", "ex.direct", "at", "amqp://root:123456@192.168.0.104:5672/")
	if err != nil {
		log.Panic().Msg("creat connect failed!")
	}
	r.PublishSimple("hello world!")
	r.ConsumeSimple()
	<-r.die
	return nil
}

func (r *RabbitMQ) Stop() {
	close(r.die)
}

//创建RabbitMQ结构体实例
func (r *RabbitMQ) NewRabbitMQCli(queueName string, exchange string, key string, mqurl string) error {
	r.QueueName = queueName
	r.Exchange = exchange
	r.Key = key
	r.Mqurl = mqurl

	var err error
	//创建rabbitmq连接
	r.conn, err = amqp.Dial(r.Mqurl)
	if err != nil {
		log.Error().Msgf("%s:%s\n", "创建连接错误", err)
		return err
	}

	r.channel, err = r.conn.Channel()
	if err != nil {
		log.Error().Msgf("%s:%s\n", "获取Channel失败", err)
		return err
	}

	return nil
}

func (r *RabbitMQ) init() error {
	if err := r.QueueDeclare(r.QueueName); err != nil {
		log.Error().Msgf("%s:%s\n", "r.QueueDeclare", err)
		return err
	}

	if err := r.ExchangeDeclare("", ""); err != nil {
		log.Error().Msgf("%s:%s\n", "r.QueueDeclare", err)
		return err
	}

	if err := r.QueueBind("", "",""); err != nil {
		log.Error().Msgf("%s:%s\n", "r.QueueBind", err)
		return err
	}

	return nil
}

//创建队列
func (r *RabbitMQ) QueueDeclare(queueName string) error {
	if queueName == "" {
		return errors.New("QueueName not found")
	}

	//1、申请消息队列，如果队列不存在会自动创建，如果存在则跳过创建
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
	if err != nil {
		log.Error().Msgf("%s:%s\n", "QueueDeclare失败", err)
		return err
	}
	return nil
}

//创建交换器
func (r *RabbitMQ) ExchangeDeclare(name, kind string) error {
	//todo，校验
	err := r.channel.ExchangeDeclare(
		//交换器名称
		name,
		//交换器类型
		kind,
		//持久化标识
		true,
		//是否自动删除
		false,
		//是否是内置交换器
		false,
		//是否等待服务器确认
		true,
		//其它配置
		nil)
	if err != nil {
		log.Error().Msgf("%s:%s\n", "ExchangeDeclare", err)
		return err
	}
	return nil
}

//绑定交换器和队列
func (r *RabbitMQ) QueueBind(name, key, exchange string) error {
	//todo，校验
	err := r.channel.QueueBind(
		//队列名称
		name,
		//根据交换器类型来设定
		key,
		//交换器名称
		exchange,
		//是否等待服务器确认
		true,
		nil)
	if err != nil {
		log.Error().Msgf("%s:%s\n", "QueueBind", err)
		return err
	}
	return nil
}

//绑定交换器（可选）
func (r *RabbitMQ) ExchangeBind(des, key, src string) error {
	//todo，校验
	err := r.channel.ExchangeBind(
		//目的交换器
		des,
		//路由键
		key,
		//源交换器
		src,
		//是否等待服务器确认
		false,
		nil)
	if err != nil {
		log.Error().Msgf("%s:%s\n", "QueueBind", err)
		return err
	}
	return nil
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
	//好处：保证队列存在，消息能发送到队列中
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
		//交换器名称
		r.Exchange,
		//路由键
		r.Key,
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
	//接收消息
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
