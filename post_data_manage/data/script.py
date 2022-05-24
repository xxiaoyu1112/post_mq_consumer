import urllib.parse
import pymongo
mongo_username = urllib.parse.quote_plus('admin')
mongo_password = urllib.parse.quote_plus('123456')
mongo_client = pymongo.MongoClient('mongodb://%s:%s@211.71.76.189:27017' % (mongo_username, mongo_password))
db_post_data = mongo_client["post_data"]
col_post_deal = db_post_data["post_deal"]
print(mongo_client.server_info()) #判断是否连接成功

def commit_to_mysql(lines:str):
    print('commit\n')
    co = []
    for line in lines:
        infos = line.split(',')
        day = infos[0]
        city = infos[2]
        post_man_id = infos[3]
        co.append({
            "raw":line,
            "post_deal_date":day,
            "city":city,
            "post_man_id":post_man_id,
            "tag":  '上海市-20200423'# 'city + "-" +day'
        })
    col_post_deal.insert_many(co)
file_path='/mnt/hgfs/share_vm/tmp/hangzhou_10blocks.csv'
# file_path='/mnt/hgfs/share_vm/tmp/test.csv'
with open(file_path) as file :
    cur_uncommit = []
    while True:
        line = file.readline()
        if not line :
            break
        line = line.replace('\n','')
        cur_uncommit.append(line)
        if len(cur_uncommit) % 2048 ==0:
            commit_to_mysql(cur_uncommit)
            cur_uncommit = []
    if len(cur_uncommit) !=0:
        commit_to_mysql(cur_uncommit)

print('finish')