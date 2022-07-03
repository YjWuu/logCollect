package taillog

import (
	"fmt"
	"logCollect/logAgent/etcd"
	"time"
)

var tskMgr *tailLogMgr

//tailTask管理者
type tailLogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &tailLogMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry),
	}
	for _, logEntry := range logEntryConf {
		tailtask := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mk] = tailtask
	}
	go tskMgr.run()
}

//监听自己的newConfChan，有了新的配置后就做对应的处理
//1.配置新增
//2.配置删除
//3.配置修改
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[mk]
				if ok {
					continue
				} else {
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			for _, c1 := range t.logEntry {
				isDelete := true
				for _, c2 := range newConf {
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[mk].cancelFunc()
				}
			}
			fmt.Println("新的配置来了", newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

//向外暴露tskMgr的某个属性
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
