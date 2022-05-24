WS=$(pwd)
REPO_PATH=$(dirname $(readlink -f "$0"))

cd $REPO_PATH
cd post_data_collect
python main.py &

cd $REPO_PATH
cd post_data_manage
python main.py &

cd $REPO_PATH
cd post_predict_consumer
go run *.go &

cd $WS
