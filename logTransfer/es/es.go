package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

//初始化es，准备接受kafka发来的数据

type LogData struct {
	Data  string `json:"data"`
	Topic string `json:"topic"`
}

var (
	client *elastic.Client
	ch     chan *LogData
)

func Init(address string, chanSize int, nums int) (err error) {
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		fmt.Println("init es err:", err)
		return
	}
	fmt.Println("ES连接成功")
	ch = make(chan *LogData, chanSize)
	for i := 0; i < nums; i++ {
		go sendToES()
	}
	return
}

func SendToESChan(msg *LogData) {
	ch <- msg
}

//往ES发送数据
func sendToES() {
	for {
		select {
		case msg := <-ch:
			put1, err := client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background())
			if err != nil {
				fmt.Println("send data to es err:", err)
				return
			}
			fmt.Printf("Indexed studetn %s to index %s , type %s\n", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}
	}

}
