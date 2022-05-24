package main

import (
	"context"
	"log"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

var HandlerMap map[string]func(context.Context, *primitive.MessageExt) (consumer.ConsumeResult, error)

func init() {
	HandlerMap = make(map[string]func(context.Context, *primitive.MessageExt) (consumer.ConsumeResult, error))
	HandlerMap["predict"] = Predict
}
func BaseHandler(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for i := range ext {
		log.Printf("subscribe callback:%v \n", ext[i])
		tag := ext[i].GetTags()
		f, con := HandlerMap[tag]
		if !con {
			log.Printf("tag solution not exits,message: %v,tag: %v \n", ext[i], tag)
		} else {
			_, err := f(ctx, ext[i])
			if err != nil {
				log.Printf("err in handler,message: %v,err: %v \n", ext[i], err)
			}
		}
	}
	return consumer.ConsumeSuccess, nil
}
