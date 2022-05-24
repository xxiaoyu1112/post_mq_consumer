package model

type PredictTask struct {
	Input        ModelPredictInput `json:"model_predict_input"`
	ModelName    string            `json:"model_name"`
	ModelVersion string            `json:"model_version"`
	TaskId       string            `json:"task_id"`
}
