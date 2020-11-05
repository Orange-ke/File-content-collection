package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

// 初始化es，准备接受kafka发过来的数据

var Client *elastic.Client

var logChan chan *LogData

type LogData struct {
	Topic string `json:"topic"`
	Data string `json:"data"`
}

func Init(address string, maxChanSize int, goNum int) (err error) {
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	Client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		fmt.Println("connect to es err: ", err)
		return
	}
	fmt.Println("connect to es success")
	// 初始化logChan
	logChan = make(chan *LogData, maxChanSize)
	for i := 0; i < goNum; i++ {
		go SendToEs()
	}
	return
}

func SendToEsChan(msg *LogData) {
	logChan <- msg
}

// 发送数据到es
func SendToEs() {
	for {
		select {
		case msg := <- logChan:
			// 链式操作
			put, err := Client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background()) // bodyJson这里的参数必须是可以json序列化的对象
			if err != nil {
				fmt.Println("send to es failed, err: ", err)
			}
			fmt.Print("获取消息")
			fmt.Printf("id: %s, index: %s, type: %s \n", put.Id, put.Index, put.Type)
		default:
			time.Sleep(time.Second)
		}
	}
}
