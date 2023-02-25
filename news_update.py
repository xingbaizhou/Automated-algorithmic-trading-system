import tushare
import datetime
import pandas as pd
import sqlalchemy
import pymysql
import re
import requests
import time
from apscheduler.schedulers.blocking import BlockingScheduler

tushare.set_token('908baa52647bedf4d86115db67ebc3ff70d9abfa4b376002f4791641')
pro = tushare.pro_api()

class Handle(object):
    def __init__(self):
        db_url = 'mysql+pymysql://root:admin@192.168.2.175/zjkj'
        self.day_str = datetime.datetime.now().strftime("%Y%m%d")

        # 入库
        self.engine = sqlalchemy.create_engine(db_url)
        # 连接数据库 取出每支股票代码
        self.data1 = pro.daily(start_date='20150101', end_date=self.day_str)
        self.list1 = []
        for i in self.data1.ts_code:
            if i not in self.list1:
                self.list1.append(i)

    def gather_news(self):
        print('start gathering news')
        # jie qu
        # t_list = t_list[44:]
        # 取出上述处理好的股票代码
        date_list = ['20210923','20210924','20210925',self.day_str]
        count = 0
        for i in self.list1:
            a = i.split('.',2)
            code = a[1].lower() + a[0]
            count += 1
            print(count)
            page = 1  # 设置页码
            while page < 6:
                index = 0  # 设置下标
                try:
                    target = "https://stocknews.cj.sina.cn/stocknews/api/news/get?device_id_spns=7df4adf5b9eab08f&symbol={}&docid=null&num=50&device_id_fake=cccabe6358546794&device_id_old=395d07f579d882b1&fr=financeapp&deviceid=19e8c1a29cfb634a&version=5.7.0.1&market=cn&chwm=34010_0001&wm=b122&ctime=null&from=7049995012&page={}".format(
                        code, page)
                    req = requests.get(target)
                    time.sleep(2)
                    texts = req.json()
                    while index < len(texts['result']['data']):
                            # 获取时间
                            date = texts['result']['data'][index]['create_date'].replace('-', '')
                            title = texts['result']['data'][index]['title']
                            # 获取docid  用于下面获取文章内容
                            docid = texts['result']['data'][index]['docid']
                            # print(date)
                            print(code)
                            if date in date_list:
                                target = "https://app.finance.sina.com.cn/toutiao/content?version=5.7.0.1&app_key=2399350321&format=json&column=news_focus&mode=raw&wapH5=y&url=https://finance.sina.com.cn/stock/s/2021-08-26/doc-{}.shtml&wm=b122&from=7049995012&chwm=34010_0001&filter=1&up_down_type=1".format(
                                    docid)
                                req = requests.get(target)
                                time.sleep(2)
                                text = req.json()
                                if text['result']['status']['code'] == 1 or 0:
                                    index += 1
                                    continue
                                content = text['result']['data'][0]['content']
                                # 处理文章内容 去除多余字符
                                contents = re.sub(r'<[^>]+>', '', content).replace('\n', '').strip()
                                df = pd.DataFrame({'datetime': [date], 'title': [title], 'content': [contents]})
                                print("prepare to add to CSV")
                                df.to_csv('~/Desktop/news/{}.csv'.format(code), mode='a+', header=True)
                                print("added to csv", i)
                                index += 1
                            else:
                                index += 1
                            page += 1
                except Exception as e:
                    print(e)

    def start(self):
        self.gather_news()


if __name__ == '__main__':
    #Handle().gather_news()
    sched = BlockingScheduler()
    sched.add_job(Handle().start, 'cron', day_of_week='mon-sun', hour=15, minute=0,)
    sched.start()