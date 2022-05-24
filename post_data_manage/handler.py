from data.mongo import convert, save_to_post_predict
from db.update_task import update_task
from process.pro_process import pre_process
from process.process import create_sample
import os
import sys
import time
import tqdm
import shutil


def write_to_file(path, data):
    if os.path.exists(path):
        os.remove(path)
    with open(path, 'w') as file:
        for item in tqdm.tqdm(data):
            line = item['raw']
            file.write(line+'\n')
            pass


ws = os.path.dirname(__file__) + '/ws/'


def do_task(task_tag, task_id):
    if '-' not in task_tag:
        return 
    [region, date] = task_tag.split('-')
    uuid = task_id
    fin = ws + '/raw_data/'+uuid+'.csv'
    fin_temp = temp_fout = ws + '/temp/' + uuid + '/'
    update_task(uuid, 2) 
    write_to_file(fin, convert(task_tag))
    pre_process(fin=fin, fout=temp_fout, is_test=False)
    update_task(uuid, 3)
    last_x, last_len, global_x, unpick_x, unpick_len, unpick_geo, days_np, order_np, index_np, eta_np, dic_geo2index = create_sample(fin_temp=fin_temp, day_window=(50, 60), block={0},
                                                                                                                                     label_range=(0, 120), len_range=(1, 25), graph_ret=False)
    update_task(uuid, 4)
    os.remove(fin)
    shutil.rmtree(fin_temp)
    save_to_post_predict(region, date, unpick_x, unpick_len, order_np)
    update_task(uuid, 5)
    print(len(unpick_x))


''''
{

    "task_id":"xxx",
    "task_tag":"xxx"
}
'''


def handler_msg(msg):
    print("[Got] :", msg)
    body = msg.body
    import json
    task = json.loads(body)
    task_id = task['task_id']
    task_tag = task['task_tag']
    do_task(task_tag, task_id)
    print("[FINISH] :", msg)
