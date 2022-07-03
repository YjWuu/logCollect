package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"logCollect/logTransfer/es"
)

//往kafka写日志的模块

func Init(addrs []string, topic string) error {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return err
	}
	fmt.Println("Kafka连接成功")
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("faid to get list of partition:err%v\n", err)
		return err
	}
	fmt.Println("分区列表:", partitionList)
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start cunsumer for partition %d, err:%v\n", partition, err)
			return err
		}
		defer pc.AsyncClose()
		go func(partitionConsumer sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("partition:%d offset:%d eky:%v value:%v\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
				//发送数据到es
				ld := es.LogData{
					Topic: topic,
					Data:  string(msg.Value),
				}
				es.SendToESChan(&ld)
			}
		}(pc)
	}
	return err
}
