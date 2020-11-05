package main

import (
	"blogProject/log_transfer/conf"
	"blogProject/log_transfer/es"
	"blogProject/log_transfer/kafka"
	"fmt"
	"gopkg.in/ini.v1"
)

// log transfer
// 将日志数据从kafka取出来发往es

var logTransferConf conf.LogTransfer

func main() {
	// 0. 加载配置文件
	err := ini.MapTo(&logTransferConf, "./conf/conf.ini")
	if err != nil {
		fmt.Println("fetch config err: ", err)
		return
	}
	fmt.Printf("cfg: %v\n", logTransferConf)
	// 2. 初始化ES
	// 2.1 初始化一个es连接的client
	err = es.Init(logTransferConf.ESConfig.Address, logTransferConf.ESConfig.ChanMaxSize, logTransferConf.ESConfig.GoNum)
	if err != nil {
		fmt.Println("init es err: ", err)
		return
	}
	// 1. 初始化kafka
	// 1.1 连接kafka，创建分区消费者
	// 1.2 每个分区的消费者分别取出数据，通过sendToEs将数据发往ES
	err = kafka.Init(logTransferConf.KafkaConfig.Address, logTransferConf.KafkaConfig.Topic)
	if err != nil {
		fmt.Println("init kafka err: ", err)
		return
	}
}
