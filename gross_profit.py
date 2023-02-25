import tushare
import datetime
import pandas as pd
import sqlalchemy
import pymysql
from math import isnan
from apscheduler.schedulers.blocking import BlockingScheduler

tushare.set_token('908baa52647bedf4d86115db67ebc3ff70d9abfa4b376002f4791641')
pro = tushare.pro_api()

db_url = 'mysql+pymysql://root:admin@192.168.2.175/zjkj'
day_str = datetime.datetime.now().strftime("%Y%m%d")
# 入库
engine = sqlalchemy.create_engine(db_url)
# 连接数据库 取出每支股票代码
conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                            charset='utf8')
data1 = pro.daily(start_date='20150101', end_date=day_str)
list1 = []
for i in data1.ts_code:
    if i not in list1:
        list1.append(i)


#list1 = list1[4:]
visit = []
for i in list1:
    print(i)
    df = pro.fina_indicator(**{
        "ts_code": "{}".format(i),
        "ann_date": "",
        "start_date": "20210630",
        "end_date": "",
        "period": "20210630",
        "update_flag": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "end_date",
        "gross_margin"
    ])
    for row in df.iterrows():
        print(row)
        print(visit)
        if row[1][2] and not isnan(row[1][2]) and row[1][0] not in visit:

            engine.execute(
                "update stock_industries set gross_profit = {} where ts_code = '{}'".format(row[1][2],
                                                                                            row[1][0]),
                con=conn)
            visit.append(row[1][0])
            print("update db",i)
