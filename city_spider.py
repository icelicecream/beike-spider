import re
from math import ceil
from random import randint
from time import sleep

import requests

from create_mongo import mongodb

# 贝壳同一时间访问多次就会触发人机验证，封锁ip
# 因此对于贝壳网来说，需要使用动态ip


class BeiKeSpider(object):
    def __init__(self):
        self.mongodb = mongodb()

    def run(self):
        # 获取全国各个链家的数据
        city_url_gen = self.get_city_url()
        try:
            while True:
                city_url = next(city_url_gen)
                city_collection = self.mongodb.create_col(city_url)
                self.get_project_data(city_url, city_collection)
        except StopIteration:
            pass
        finally:
            print('链接已全部请求完毕')

    # 获取总页数
    def get_total_project_num(self, city_url):
        req = requests.get(f'{city_url}/pg1/?_t=1')
        return req.json()['data']['total']

    # 制作一个城市链接地址的迭代器
    def get_city_url(self):
        url = 'https://hz.fang.ke.com/loupan'
        req = requests.get(url)
        city_url_regexp = re.compile(
            '(?!<a href=")\/\/.{1,15}\.fang\.ke\.com\/loupan')
        city_url_array = list(set(re.findall(city_url_regexp, req.text)))
        city_url_length = len(city_url_array)
        i = 0
        while i < city_url_length:
            yield 'https:' + city_url_array[i]
            i = i + 1

    # 多线程下载每一页的项目信息(同时下载，并没有间隔一定时间)
    def get_project_data(self,
                         city_url,
                         city_collection,
                         start_page=1,
                         end_page=None):
        if not end_page:
            end_page = ceil(int(self.get_total_project_num(city_url)) / 10)
        for page in range(start_page, end_page + 1):
            self.get_data_from_page(city_url, page, city_collection)
            print(f'下载了第{page}页的数据')
            # 进行适量的延时，避免被封ip
            sleep(randint(300, 700) / 100)
            # Thread(target=self.get_data_from_page,
            #        args=(city_url, page, city_collection)).start()
        print('全部下载完了')

    # 处理每一页拿到的项目信息
    def get_data_from_page(self, city_url, page, city_collection):
        req = requests.get(f'{city_url}/pg{str(page)}/?_t=1')
        project_list = req.json()['data']['list']
        self.mongodb.insert_data(city_collection, project_list)


def main():
    beike = BeiKeSpider()
    beike.run()


if __name__ == "__main__":
    main()
