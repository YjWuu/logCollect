package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"logCollect/logAgent/kafka"
)

//从日志文件收集日志的模块

type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	//为了能实现退出t.run()
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	err := tailObj.init()
	if err != nil {
		fmt.Println("tailObj init err:", err)
	}
	return
}

func (t *TailTask) init() (err error) {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
		return
	}
	//当gouroutine执行的函数退出的时候，goroutine就结束了
	go t.run() //直接收集日志发送到kafka
	return
}

func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task:%s_%s 结束了\n", t.path, t.topic)
			return
		case line := <-t.instance.Lines:
			//kafka.SendToKafka(t.topic, line.Text)
			//先把日志数据发送到一个通道中
			fmt.Printf("get log data from %s success, log:%v\n", t.path, t.topic)
			kafka.SendToChan(t.topic, line.Text)
			//kafka那个包中有单独的goroutine去收取日志发送到kafka
		}
	}
}

//func Init(fileName string) (err error) {
//	config := tail.Config{
//		ReOpen:    true,
//		Follow:    true,
//		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
//		MustExist: false,
//		Poll:      true,
//	}
//	tailObj, err = tail.TailFile(fileName, config)
//	if err != nil {
//		fmt.Println("tail file failed, err:", err)
//		return
//	}
//	return
//}
