package main

import (
	"blogProject/log_agent/conf"
	"blogProject/log_agent/etcd"
	"blogProject/log_agent/kafka"
	"blogProject/log_agent/taillog"
	"fmt"
	"gopkg.in/ini.v1"
	"sync"
	"time"
)

var cfg = new(conf.AppConfig)

// logAgent 入口程序
func main() {
	// 0. 加载配置文件
	err := ini.MapTo(cfg, "./conf/config.ini")
	// 1. 初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConfig.Address}, cfg.ChanMaxSize)
	if err != nil {
		fmt.Println("init kafka failed, err: ", err)
		return
	}
	fmt.Println("init kafka success")
	// 2. 初始化etcd
	err = etcd.Init(cfg.EtcdConfig.Address, time.Duration(cfg.EtcdConfig.Timeout)*time.Second)
	if err != nil {
		fmt.Println("init etcd failed, err: ", err)
		return
	}
	// 2.1 从etcd中获取日志收集项的配置信息
	key := fmt.Sprintf(cfg.EtcdConfig.Key, cfg.LocalConfig.Ip)
	logEntries, err := etcd.GetConf(key)
	if err != nil {
		fmt.Println("etcd.GetConf err: ", err)
		return
	}
	for _, val := range logEntries {
		fmt.Printf("%v %v\n", val.Path, val.Topic)
	}
	fmt.Println("init etcd success")
	// 3. 收集日志发往kafka
	taillog.Init(logEntries)
	// 因为NewConfChan 访问了taskMgr的NewConfChan，这个channel是再taillog.Init(logEntryConf)执行的初始化
	newConfChan := taillog.NewConfChan()
	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchConf(key, newConfChan)
	wg.Wait()
}
