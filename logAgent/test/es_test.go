package test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"testing"
)

type Student struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Married bool   `json:"married"`
}

func TestES(t *testing.T) {
	//初始化连接
	client, err := elastic.NewClient(elastic.SetURL("http://127.0.0.1:9200"))
	if err != nil {
		fmt.Println("connect es err:", err)
		return
	}
	fmt.Println("connect es success")
	s1 := Student{
		Name:    "rion",
		Age:     22,
		Married: false,
	}
	put1, err := client.Index().Index("student").Type("go").BodyJson(s1).Do(context.Background())
	if err != nil {
		fmt.Println("index err:", err)
		return
	}
	fmt.Printf("Indexed student %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
}
