package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/v3/clientv3"
	"time"
)

var (
	Cli *clientv3.Client
)

type LogEntry struct {
	Path  string `json:"path"`  // 日志存放的路径
	Topic string `json:"topic"` // 日志要发往kafka中的那个Topic
}

// 初始化etcd的函数
func Init(addr string, timeout time.Duration) (err error) {
	Cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeout,
	})
	if err != nil {
		fmt.Println("connect to etcd err: ", err)
		return
	}
	return
}

// 从etcd中获取配置项
func GetConf(key string) (logEntries []*LogEntry, err error) { // 可能存在多个配置文件
	// get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := Cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Println("get from etcd failed, err:", err)
		return
	}
	for _, ev := range resp.Kvs {
		err := json.Unmarshal(ev.Value, &logEntries) // 使用json格式解析
		if err != nil {
			fmt.Println("unmarshal etcd value failed, err: ", err)
			return nil, err
		}
	}
	return
}

func WatchConf(key string, newConfChan chan<- []*LogEntry) {
	ch := Cli.Watch(context.Background(), key)
	// 从通道获取值
	var newConf []*LogEntry
	for resp := range ch {
		for _, ev := range resp.Events {
			fmt.Printf("Type: %s key: %s value: %v\n", ev.Type, string(ev.Kv.Key), string(ev.Kv.Value))
			// 如果是delete 则传入空的[]*LogEntry
			// 如果是put则将新的放入
			err := json.Unmarshal(ev.Kv.Value, &newConf)
			if err != nil {
				fmt.Println("unmarshal etcd value failed, err: ", err)
				return
			}
			newConfChan <- newConf
		}
	}
}
