from rocketmq.client import PullConsumer
from data.mongo import convert, save_to_post_predict
from process.pro_process import pre_process
from process.process import create_sample
import os
import sys
import time
import tqdm
import shutil


from handler import *


cur_path = os.path.abspath(__file__)
sys.path.append(cur_path + "/process")


# do_task('上海市-20200423')


if __name__ == '__main__':
    import sys
    print(ws)
    # if len(sys.argv) == 2 and sys.argv[0] == 'prod':
    ws = os.path.dirname(__file__) + '/ws/'
    print(ws)
    consumer = PullConsumer('post_data_manage_consumer')
    consumer.set_namesrv_addr('211.71.76.189:9876')
    consumer.start()
    while True:
        for msg in consumer.pull('post_data_manage'):
            try:
                handler_msg(msg)
            except:
                print("An exception occurred")
        time.sleep(1)  # 防止CPU空转
        print("[consumer is runing] %s" %
              time.asctime(time.localtime(time.time())))
    print(__name__)
# consumer.shutdown()
