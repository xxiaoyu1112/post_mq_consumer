package model

// Redis中存在的结果结构体
type PredictTaskResult struct {
	TaskId     string  `json:"task_id"`
	TaskStatus int32   `json:"task_status"`
	TaskResult []int32 `json:"task_result"`
}

const PredictTaskResultStatusWaitting = 1 // 未处理完成
const PredictTaskResultStatusError = 2    // 处理完成,但是存在错误
const PredictTaskResultStatusFinish = 3   // 处理完成,结果完成

func BuildErrorPredictTaskResult(taskId string) *PredictTaskResult {
	return &PredictTaskResult{
		TaskId:     taskId,
		TaskStatus: PredictTaskResultStatusError,
	}
}

func BuildSuccessPredictTaskResult(taskId string, res *ModelPredictOutput) *PredictTaskResult {
	return &PredictTaskResult{
		TaskId:     taskId,
		TaskStatus: PredictTaskResultStatusError,
		TaskResult: res.Order,
	}
}
