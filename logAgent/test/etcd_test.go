package test

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestEtcd(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd filed, err: ", err)
		return
	}
	fmt.Println("connect to etcd success")
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	value := `[{"path":"c:/tmp/nginx.log","topic":"web_log"},{"path":"d:/xxx/redis.log","topic":"redis_log"}]`
	_, err = cli.Put(ctx, "/xxx", value)
	//value := `[{"path":"c:/tmp/nginx.log","topic":"web_log"},{"path":"d:/xxx/redis.log","topic":"redis_log"}]`
	//_, err = cli.Put(ctx, "/logagent/192.168.94.16/collect_config", value)
	cancel()
	if err != nil {
		fmt.Println("put to etcd err:", err)
		return
	}
}
