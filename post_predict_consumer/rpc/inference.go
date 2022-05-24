package rpc

import (
	"context"
	"encoding/json"
	"log"
	"post_predict_consumer/model"
	pb "post_predict_consumer/rpc/rpc_idl/torch_serve"

	"google.golang.org/grpc"
)

const (
	inference_address = "211.71.76.189:7070"
)

var inference_client pb.InferenceAPIsServiceClient

func init() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(inference_address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	inference_client = pb.NewInferenceAPIsServiceClient(conn)
}
func Predict(ctx context.Context, modelName, modelVersion string, modelInput *model.ModelPredictInput) (*model.ModelPredictOutput, error) {
	body, err := json.Marshal(modelInput)
	if err != nil {
		return nil, err
	}
	input := make(map[string][]byte)
	input["body"] = []byte(body)
	req := pb.PredictionsRequest{
		ModelName:    modelName,
		ModelVersion: modelVersion,
		Input:        input,
	}
	resp, err := inference_client.Predictions(ctx, &req)
	if err != nil {
		log.Printf("error:%v", err)
		return nil, err
	}
	modelPredictOutput := model.ModelPredictOutput{
		Order: make([]int32, 0),
	}
	json.Unmarshal(resp.Prediction, &modelPredictOutput.Order)
	log.Printf("opt:%v", modelPredictOutput)
	return &modelPredictOutput, nil
}
