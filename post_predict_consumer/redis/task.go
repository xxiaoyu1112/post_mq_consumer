package redis

import (
	"context"
	"encoding/json"
	"post_predict_consumer/model"
	"time"
)

const predictTaskPrefix = "predict:"

func SetPredictTaskResult(ctx context.Context, res *model.PredictTaskResult) error {
	resMsg, err := json.Marshal(res)
	if err != nil {
		return err
	}
	key := predictTaskPrefix + res.TaskId
	val := string(resMsg)
	err = rdb.Set(ctx, key, val, time.Hour).Err()
	if err != nil {
		return err
	}
	return nil
}
