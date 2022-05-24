

import os
import sys
import time

from rocketmq.client import PullConsumer
from handler.handler import handle_data_msg

consumer = PullConsumer('post_data_manage_consumer')
consumer.set_namesrv_addr('211.71.76.189:9876')
consumer.start()

while True :
    for msg in consumer.pull('post_data_collect'):
      try:
         handle_data_msg(msg)
      except:
          print("An exception occurred")
     
    time.sleep(1) # 防止CPU空转
    print("[consumer is runing] %s"%time.asctime( time.localtime(time.time()) ))