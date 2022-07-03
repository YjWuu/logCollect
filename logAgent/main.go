package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logCollect/logAgent/conf"
	"logCollect/logAgent/etcd"
	"logCollect/logAgent/kafka"
	"logCollect/logAgent/taillog"
	"logCollect/logAgent/utils"
	"sync"
	"time"
)

var (
	cfg = new(conf.AppConf)
)

func main() {
	//0.加载配置文件
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Println("load ini failed, err", err)
		return
	}

	//1.初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.ChanMaxSize)
	if err != nil {
		fmt.Println("init kafka failed, err:", err)
	}
	fmt.Println("init kafka success")

	//2.初始化etcd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("init etcd failed, err:", err)
	}
	fmt.Println("init etcd success")
	ipStr, err := utils.GetOutboundIP()
	if err != nil {
		fmt.Println(err)
		return
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	//2.1从etcd中获取日志收集项目的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Println("etcd get conf failed, err:", err)
		return
	}
	fmt.Printf("get conf from etcd success, %v\n", logEntryConf)
	//2.2派一个哨兵去监视日志收集项的变化（热加载）
	for index, value := range logEntryConf {
		fmt.Printf("index:%v value:%v\n", index, value)
	}

	//3.收集日志发往kafka
	taillog.Init(logEntryConf)
	newConfChan := taillog.NewConfChan() // 从taillog包中获取对外暴露的通道
	var wg sync.WaitGroup
	wg.Add(1)
	//哨兵发现最新的配置信息会通知上面的那个通道
	go etcd.WatchConf(etcdConfKey, newConfChan)
	wg.Wait()
}
