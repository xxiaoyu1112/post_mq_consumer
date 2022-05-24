package main

import (
	"context"
	Handler "post_predict_consumer/handler"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func Predict(ctx context.Context, msg *primitive.MessageExt) (consumer.ConsumeResult, error) {
	handler := Handler.NewPredictHanler(ctx, msg)
	handler.Run()
	return handler.Res, nil
}
