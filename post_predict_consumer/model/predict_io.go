package model

type ModelPredictInput struct {
	Feature [][]float32 `json:"feature"`
	Start   int32       `json:"start"`
}
type ModelPredictOutput struct {
	Order []int32 `json:"order"`
}
