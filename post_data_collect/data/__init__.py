import pymongo
import numpy as np
import urllib.parse
mongo_username = urllib.parse.quote_plus('admin')
mongo_password = urllib.parse.quote_plus('123456')
mongo_client = pymongo.MongoClient('mongodb://%s:%s@211.71.76.189:27017' % (mongo_username, mongo_password))
db_post_data = mongo_client["post_data"]
col_post_deal = db_post_data["post_deal"]