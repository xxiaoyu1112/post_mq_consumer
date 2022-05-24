package main

import (
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
)

const mqAddress = "211.71.76.189:9876"
const groupName = "post_predict_task"
const topic = "post_model_predict_task"

func main() {

	// 设置推送消费者
	c, _ := rocketmq.NewPushConsumer(
		//消费组
		consumer.WithGroupName(groupName),
		// namesrv地址
		consumer.WithNameServer([]string{mqAddress}),
	)
	defer c.Shutdown()
	// 必须先现在 开始前
	err := c.Subscribe(topic, consumer.MessageSelector{}, BaseHandler)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Start()
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}
