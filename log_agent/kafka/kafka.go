package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// 实现往kafka中写日志
var (
	client sarama.SyncProducer // 声明一个全局的kafka的生产者client
	logChan chan *logData
)

type logData struct {
	topic string
	data string
}

// 初始化client
func Init(addr []string, maxSize int) (err error) {
	config := sarama.NewConfig()
	// 设置配置项
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follower都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true // 成功交付的消息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		fmt.Println("producer closed, err: ", err)
		return
	}
	// 初始化通道
	logChan = make(chan *logData, maxSize)
	// 开启后台的goroutine从通道中取数据发到kafka
	go SendToKafka()
	return
}

// 不断的从管道中取日志消息，真正的发往kafka
func SendToKafka() {
	for {
		select {
		case line := <-logChan:
			// 构造一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = line.topic
			msg.Value = sarama.StringEncoder(line.data)
			// 发送消息
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("Send msg failed. err: ", err)
				return
			}
			fmt.Printf("pid: %v offset: %v\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 50) // 休息50毫秒
		}
	}
}

// 给外部暴露一个函数，该函数把日志数据发送到一个内部的channel中
func SendToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data: data,
	}
	logChan <- msg
}
