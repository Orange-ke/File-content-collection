package taillog

import (
	"blogProject/log_agent/kafka"
	"context"
	"fmt"
	"github.com/hpcloud/tail"
)

// 从日志文件中收集日志的模块

// TailTask: 一个日志收集的任务
type TailTask struct {
	path string
	topic string
	instance *tail.Tail
	ctx context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path string, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path: path,
		topic: topic,
		ctx: ctx,
		cancelFunc: cancel,
	}
	tailObj.init() // 根据路径去打开对应的日志文件
	return
}

func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件那个地方开始读
		MustExist: false,                                // 文件不存在报错？
		Poll:      true,                                 // 拉取
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err : ", err)
		return
	}
	// 当配置中删除后，利用context停掉对应的配置项读取
	// 利用run方法退出则退出
	go t.run() // 直接采集去发送日志数据到kafka包中的消息通道中
}


func (t *TailTask) run() {
	for {
		select {
		case <- t.ctx.Done():
			fmt.Println("tail task: 结束了...", t.path, t.topic)
			return
		case line := <-t.instance.Lines:
			// 发往kafka
			//kafka.SendToKafka(t.topic, line.Text)
			// 先把日志数据发到一个通道中
			kafka.SendToChan(t.topic, line.Text)
			// kafka那个包中有单独的goroutine去取日志数据发到kafka
		}
	}
}
