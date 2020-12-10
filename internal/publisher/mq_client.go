package publisher

import (
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"time"
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
	queueName := viper.GetString("RabbitMQ.QueueName")
	exchange := viper.GetString("RabbitMQ.Exchange")
	key := viper.GetString("RabbitMQ.Key")
	mqurl := viper.GetString("RabbitMQ.Mqurl")
	log.Info().Msgf("queueName:%s,exchange:%s,key:%s,mqurl:%s", queueName, exchange, key, mqurl)

	err := r.NewRabbitMQCli(queueName, exchange, key, mqurl)
	if err != nil {
		log.Panic().Msgf("creat connect failed!,error:%s", err.Error())
	}
	defer r.Destory()

	err = r.init()
	if err != nil {
		log.Panic().Msg(err.Error())
	}

	go func() {
		for {
			err = r.PublishSimple("hello world!" + time.Now().String())
			if err != nil {
				log.Panic().Msgf("PublishSimple failed!,error:%s", err.Error())
			}
			time.Sleep(2 * time.Second)
		}
	}()

	//r.ConsumeSimple()
	<-r.die
	return nil
}

func (r *RabbitMQ) Stop() {
	close(r.die)
}

//创建RabbitMQ结构体实例
func (r *RabbitMQ) NewRabbitMQCli(queueName string, exchange string, key string, mqurl string) error {
	if queueName == "" || exchange == "" || key == "" || mqurl == "" {
		return errors.New("params is invalid")
	}
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

	if err := r.ExchangeDeclare(r.Exchange, "direct"); err != nil {
		log.Error().Msgf("%s:%s\n", "r.QueueDeclare", err)
		return err
	}

	if err := r.QueueBind(r.QueueName, r.Key, r.Exchange); err != nil {
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
func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

//简单模式Step：2、简单模式下生产代码
func (r *RabbitMQ) PublishSimple(message string) error {
	//此处队列已提前申请，若无则需要申请
	//2.发送消息到队列中
	err := r.channel.Publish(
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
		return err
	}
	log.Debug().Msgf("publish:%s\n", message)
	return nil
}

//简单模式Step：3、消费
func (r *RabbitMQ) ConsumeSimple() error {
	//此处队列已提前申请，若无则需要申请
	//接收消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		//用来区分多个消费者
		"a",
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
	if err != nil {
		log.Error().Msgf("%s:%s\n", "consume", err)
		return err
	}

	closeChan := make(chan *amqp.Error, 1)
	notifyClose := r.channel.NotifyClose(closeChan) //一旦消费者的channel有错误，产生一个amqp.Error，channel监听并捕捉到这个错误
	closeFlag := false
	forever := make(chan bool)

	//3、启动协程处理消息
	go func() {
		/*for d := range msgs {
			//实现我们要处理的逻辑函数
			log.Printf("Received a message : %s", d.Body)
		}*/
		for {
			select {
			case e := <-notifyClose:
				log.Error().Msgf("chan通道错误,e:%s", e.Error())
				close(closeChan)
				time.Sleep(1 * time.Second)
				r.ConsumeSimple()
				closeFlag = true
			case msg := <-msgs:
				//实现我们要处理的逻辑函数
				log.Printf("Received a message : %s", msg.Body)

			}
			if closeFlag {
				break
			}
		}
	}()

	log.Printf("[*] Waiting for messagees,To exit press CTRL+C")

	<-forever
	return nil
}
