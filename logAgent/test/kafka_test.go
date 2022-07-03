package test

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
)

func TestSendToKafka(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	msg := &sarama.ProducerMessage{}
	msg.Topic = "web_log"
	msg.Value = sarama.StringEncoder("this is a test log")
	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	fmt.Println("Kafka连接成功")
	defer client.Close()
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)
	fmt.Println("发送数据成功")
}

func TestReceiveFromKafka(t *testing.T) {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	fmt.Println("Kafka连接成功")
	partitionList, err := consumer.Partitions("web_log")
	if err != nil {
		fmt.Printf("faid to get list of partition:err%v\n", err)
		return
	}
	fmt.Println("分区列表:", partitionList)
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("web_log", int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start cunsumer for partition %d, err:%v\n", partition, err)
			return
		}
		defer pc.AsyncClose()
		go func(partitionConsumer sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("partition:%d offset:%d eky:%v value:%v", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
			}
		}(pc)
	}
	fmt.Println("成功消费数据")
}
