import json
from data.upload_deal import upload_post_deal
'''
    # 需要优化,根据uuid进行在数据库去重
    assert body
    {
        "uuid":"xxxx",
        "task_content":"20191114,13603,上海市,2206587620542,2019-11-14 07:49:42,2019-11-14 09:00:00,2019-11-14 11:00:00,121.409836,31.330885,24578197,3110,城中村,2019-11-14 09:56:58,2019-11-14 10:01:58,121.4101478407118,31.327883843315973,3.700000047683716,,,,"
    }
'''


def handle_data_msg(msg):
    body = msg.body
    print('got with msg:%s', body)
    data = json.loads(body)
    print('ok')
    task_content = data['task_content']
    line = task_content
    infos = line.split(',')
    day = infos[0]
    city = infos[2]
    post_man_id = infos[3]
    upload_item = {
            "raw":line,
            "post_deal_date":day,
            "city":city,
            "post_man_id":post_man_id,
            "tag":  '上海市-20200423'# 'city + "-" +day'
    }
    upload_post_deal(upload_item)
    print('deal with msg:%s', body)

