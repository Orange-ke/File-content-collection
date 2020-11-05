package kafka

import (
	"blogProject/log_transfer/es"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

func Init(address string, topic string) error {
	consumer, err := sarama.NewConsumer([]string{address}, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return err
	}
	fmt.Println(topic)
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return err
	}
	var wg sync.WaitGroup
	wg.Add(len(partitionList))
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v \n", partition, err)
			return err
		}
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			defer pc.AsyncClose()
			fmt.Println("开启消费者")
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%s \n", msg.Partition, msg.Offset, msg.Key, msg.Value)
				// 这里写给kafka
				// 构建LogData
				ld := &es.LogData{
					Data: string(msg.Value),
					Topic: topic,
				}
				// 然后发给es
				fmt.Printf("%s, %s", ld.Data, ld.Topic)
				es.SendToEsChan(ld) // 这里存在函数调函数的情况，最好也使用异步的方式
				if err !=nil {
					fmt.Println("send to es failed, err: ", err)
					continue
				}
			}
		}(pc)
	}
	wg.Wait()
	return err
}
