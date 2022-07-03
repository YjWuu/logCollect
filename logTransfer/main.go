package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logCollect/logTransfer/conf"
	"logCollect/logTransfer/es"
	"logCollect/logTransfer/kafka"
)

//将日志数据从kafka取出来发往ES

func main() {
	//0.加载配置文件
	var cfg conf.LogTransfer
	err := ini.MapTo(&cfg, "./conf/config.ini")
	if err != nil {
		fmt.Println("init config, err:", err)
		return
	}
	fmt.Println("cfg:", cfg)

	//1.初始化es
	err = es.Init(cfg.ESCfg.Address, cfg.ESCfg.ChanSize, cfg.ESCfg.Nums)
	if err != nil {
		return
	}

	//2.1初始化kafka
	//2.2连接kafka，创建分区的消费者
	//2.3每个分区的消费者分别取出数据，通过sendToES函数将数据发往es
	err = kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic)
	if err != nil {
		fmt.Println("init kafka err:", err)
		return
	}
}
