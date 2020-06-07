import re
from json import load

from pymongo import MongoClient

# 利用城市链接创建不同城市的collection，并且在里面插入多个项目
# client = MongoClient('mongodb://127.0.0.1:27017')
# db = client.beike
# city = db.create_collection('gz')
# city.insert_one({'project_name': 'xxxxx', 'project_pos': 'xxxx'})

# 1、按照链接提取城市的缩写，生成collection
# 2、在collection中插入项目信息


class mongodb(object):
    def __init__(self):
        client = MongoClient('mongodb://127.0.0.1:27017')
        self.db = client.beike

    # 生成指定城市名的collection
    def create_col(self, city_url):
        city_short = re.split('\.', city_url[8:])[0]
        if city_short in self.db.list_collection_names():
            return self.db.get_collection(city_short)
            # self.db.drop_collection(city_short)
        city = self.db.create_collection(city_short)
        return city

    # 插入数据 -- project_list指的是解析网站回馈的json后拿到的项目列表
    def insert_data(self, collection, project_list):
        with open('./keywords.json', 'rb') as json_data:
            data = load(json_data)['keywords']
        for project in project_list:
            obj = {}
            for kw in data:
                obj[kw['kw']] = project[kw['data_kw']]
            collection.insert_one(obj)


# from test_data import data
# with open('./test_data.json', 'rb') as test_data:
#     dt = load(test_data)
# mg = mongodb()
# city = mg.create_col('https://testdata.fang.ke.com/loupan')
# mg.insert_data(city, dt['data']['list'])
