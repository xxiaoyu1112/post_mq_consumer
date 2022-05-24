package handler

import (
	"context"
	"encoding/json"
	"log"
	"post_predict_consumer/model"
	"post_predict_consumer/redis"
	"post_predict_consumer/rpc"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type PredictHanler struct {
	Msg *primitive.MessageExt  // req
	Res consumer.ConsumeResult // resp
	Ctx context.Context
}

func NewPredictHanler(Ctx context.Context, Msg *primitive.MessageExt) *PredictHanler {
	return &PredictHanler{
		Msg: Msg,
		Res: consumer.ConsumeSuccess,
		Ctx: Ctx,
	}
}
func (h *PredictHanler) Run() {
	log.Printf("[PredictHanler] start with msg:%v", h.Msg)
	task := h.GetPredictTask()
	output, err := rpc.Predict(h.Ctx, task.ModelName, task.ModelVersion, &task.Input)
	if err != nil {
		log.Printf("[PredictHanler] call Rpc Predict err:%v", err)
		err := h.SetTaskError(task)
		if err != nil {
			log.Printf("[PredictHanler] SetTaskError err:%v", err)
		}
		return
	}
	err = h.SetTask(task, output)
	if err != nil {
		log.Printf("[PredictHanler] SetTaskError err:%v", err)
	}
}

func (h *PredictHanler) GetPredictTask() *model.PredictTask {
	var task model.PredictTask
	err := json.Unmarshal(h.Msg.Body, &task)
	if err != nil {
		log.Printf("[PredictHanler] get GetPredictTask Error,err:%v", err)
		return nil
	}
	return &task
}
func (h *PredictHanler) SetTaskError(task *model.PredictTask) error {
	taskResult := model.BuildErrorPredictTaskResult(task.TaskId)
	return redis.SetPredictTaskResult(h.Ctx, taskResult)
}
func (h *PredictHanler) SetTask(task *model.PredictTask, res *model.ModelPredictOutput) error {
	taskResult := model.BuildSuccessPredictTaskResult(task.TaskId, res)
	return redis.SetPredictTaskResult(h.Ctx, taskResult)
}
