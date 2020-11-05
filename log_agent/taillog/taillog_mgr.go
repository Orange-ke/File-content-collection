package taillog

import (
	"blogProject/log_agent/etcd"
	"fmt"
	"time"
)

var TaskMgr *taillogMgr

type taillogMgr struct {
	logEntry []*etcd.LogEntry
	taskMap map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logConf []*etcd.LogEntry) {
	TaskMgr = &taillogMgr{
		logEntry: logConf,
		taskMap: make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry),
	}
	for _, logEntry := range logConf {
		// 3.1 循环每一个配置项，创建对应的tailObj
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		// 初始化的时候起了多少tailTask都要记下来，为了后续判断方便
		TaskMgr.taskMap[mk] = tailObj
	}
	go TaskMgr.run()
}

// 监听自己的newConfChan，有了新的配置过来做对应的处理
func (t *taillogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			// 1. 配置新增
			// 3. 配置变更
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.taskMap[mk]
				if ok {
					// 原来就有，不需要操作
					continue
				} else {
					// 新增或变更
					tailObj := NewTailTask(conf.Path, conf.Topic)
					mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
					t.taskMap[mk] = tailObj
				}
			}
			// 找出t.taskMap有，但是newConf中没有的，要删除掉
			// 2. 配置删除
			for _, c1 := range t.logEntry { // 从原来的配置中一次拿出配置项
				isDelete := true
				for _, c2 := range newConf { // 与新的配置逐一对比
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// 把c1对一个的这个tailObj给停掉
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.taskMap[mk].cancelFunc()
				}
			}
			fmt.Println("新配置来了： ", newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// 一个函数，向外暴力taskMgr的newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return TaskMgr.newConfChan
}
