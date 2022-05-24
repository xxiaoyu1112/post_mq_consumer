import pymongo
import numpy as np
import urllib.parse
mongo_username = urllib.parse.quote_plus('admin')
mongo_password = urllib.parse.quote_plus('123456')
mongo_client = pymongo.MongoClient(
    'mongodb://%s:%s@211.71.76.189:27017' % (mongo_username, mongo_password))
db_post_data = mongo_client["post_data"]
col_post_deal = db_post_data["post_deal"]
col_post_predict = db_post_data["post_predict"]
print(mongo_client.server_info())  # 判断是否连接成功
# x = col_post_deal.find({  "_id": "61b95fb11409fafbc6b45662"})

# print(x)


def get_post_deal_by_tag(tag: str, limit: int = -1):
    if limit == -1:
        return col_post_deal.find({"tag": tag})
    return col_post_deal.find({"tag": tag}).limit(limit)


def get_all_post_deal():
    return col_post_deal.find()


def get_post_deal_by_date_and_region(deal_date: str, deal_region: str, limit: int = -1):
    if limit == -1:
        return col_post_deal.find({"city": deal_region, "post_deal_date": deal_date})
    return col_post_deal.find({"city": deal_region, "post_deal_date": deal_date}).limit(limit)


def convert(tag):
    #res = get_post_deal_by_date_and_region('20210401','杭州市')
    res = get_post_deal_by_tag(tag)
    print(res)
    return res
# x['接单最近时间'].fillna(value = np.nan)
# x['接单最近经度'].fillna(value = np.nan)
# x['接单最近纬度'].fillna(value = np.nan)
# x['接单轨迹精度'].fillna(value = np.nan)
# print(x.iloc[0])

# print(len(l_res))
# col_post_deal.aggregate( [{"$group": { "_id": { city: "$city", post_deal_date: "$post_deal_date" } } }]);
# printjson(result);

# pro_process()


def save_to_post_predict(region, date, unpick_x, unpick_len, order_np):
    l_insert = []
    for i in range(len(unpick_len)):
        len_ = unpick_len[i].item()
        x = unpick_x[i][:len_].tolist()
        order = order_np[i][:len_].tolist()
        x = {
            "points": x,
            "label": order,
            "start": order[0],
            "length": len_,
            "region": region,
            "deal_date": date
        }
        print('---')
        print(type(x))
        print(type(order))
        print(type(order[0]))
        print(type(len_))
        print(type(date))
        l_insert.append(x)
        # break
    if len(l_insert) == 0:
        return
    col_post_predict.insert_many(l_insert)


# n = np.array([1, 2, 3])
# print(type(n[0].item()))
# print(n[0])
# # n = n.tolist()
# x = {
#     "points": n
# }

# col_post_predict.insert_many([x])
