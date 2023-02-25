import tushare
import datetime
import pandas as pd
import sqlalchemy
import pymysql
from math import isnan
import requests
import time
import json
import random
import re
from bs4 import BeautifulSoup
import pandas
import csv
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
        self.conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                               charset='utf8')
        self.data1 = pro.daily(start_date='20150101', end_date=self.day_str)
        self.list1 = []
        for i in self.data1.ts_code:
            if i not in self.list1:
                self.list1.append(i)

        stock = self.engine.execute("show tables").fetchall()
        stock = stock[:-12]
        stock = stock[1:]
        self.stock_list = []
        for s in stock:
            self.stock_list.append(s[0])

        self.symbols=[]
        for s in stock:
            p = s[0].split('_')
            self.symbols.append(p[0])



    def moving_average(self):
        """
        moving average 5,10,15,20
        :return:
        """
        # try:
        #     for code in self.list1:
        # except Exception as e:
        #     print(e)
        pass

    def day_range(self,bgn, end):
        fmt = '%Y%m%d'
        begin = datetime.datetime.strptime(bgn, fmt)
        end = datetime.datetime.strptime(end, fmt)
        delta = datetime.timedelta(days=1)
        interval = int((end - begin).days) + 1
        return [datetime.datetime.strftime(begin + delta * i,'%Y%m%d') for i in range(0, interval, 1)]

    def q(self):
        """
        日线行情
        :return:
        """
        try:
            self.list1 = self.list1[1:]
            for i in self.list1:
                print(i)
                try:
                    data = pro.daily(ts_code=i, start_date=self.day_str, end_date=self.day_str)
                    print(data)
                    if not data.empty:
                        pd.io.sql.to_sql(data, i.replace('.', '_').lower(), self.engine, schema='zjkj', if_exists='append',
                                         index=False)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)


    def net(self):
        """
        资金流向
        :return:
        """
        list2 = []
        try:
            self.engine.execute(
                'alter table {} add inflow float default null'.format('301077_sz'))
        except Exception as e:
            print(e)
        finally:
            try:
                df = pro.moneyflow(ts_code='301077_sz', start_date='20210930', end_date = self.day_str)
                for k in df.net_mf_amount:
                    list2.append(k)
                o = 0
                for j in df.trade_date:
                    self.engine.execute('update {} set inflow = {} where trade_date = {}'.format(
                        '301077_sz', list2[o], j))
                    o += 1
            except Exception as x:
                print(x)

    def market(self):
        """
        流通市值
        :return:
        """
        list2 = []
        try:
            self.engine.execute('alter table {} add market_equity float(100,8) default null'.format('301077_sz'),con = self.conn)
        except Exception as e:
            print(e)
        finally:
            try:
                df = pro.daily_basic(ts_code='{}'.format(self.list1[i]), start_date='20210930', end_date=self.day_str)
                for k in df.circ_mv:
                    list2.append(k)
                o = 0
                for j in df.trade_date:
                    self.engine.execute('update {} set market_equity = {} where trade_date = {}'.format(
                        '301077_sz', list2[o], j))
                    o += 1
            except Exception as x:
                print(x)


    def update_ma_daily(self, num2):
        for stock in self.stock_list:
            try:
                self.engine.execute("alter table {} add ma5 double".format(stock), con=self.conn)
                self.engine.execute("alter table {} add ma60 double".format(stock), con=self.conn)
                self.engine.execute("alter table {} add ma_5_60_delta double".format(stock), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    stock_data = pd.read_sql("select * from {} order by trade_date desc limit {}".format(num2+1), con=self.conn)
                    ma_update = stock_data['close'].rolling(num2).mean()
                    for row in ma_update.items():
                        if not isnan(row[1]):
                            trade_date = row[0].strftime("%Y%m%d")
                            value = row[1]
                            self.engine.execute(
                                "update {} set ma{} = {} where trade_date = {} limit {}".format(stock, num2, value, trade_date,
                                                                                               self.day_str,num2))
                except Exception as e:
                    print(e)

    def update_ma_delta(self, num1, num2):
        for stock in self.stock_list:
            try:
                self.engine.execute("alter table {} add ma_{}_{}_delta double".format(stock,num1,num2), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    self.engine.execute(
                        "update {} set ma_{}_{}_delta = (ma{}-ma{})/ma{} where trade_date = {}".format(stock, num1, num2,
                                                                                                       num1, num2, num2,
                                                                                                       self.day_str),
                        con=self.conn)
                except Exception as e:
                    print(e)

    def Dfnews(ts_code):
        time_str = str(int(time.time() * 1000))
        time.sleep(0.001)

        url = "https://searchapi.eastmoney.com/bussiness/Web/GetCMSSearchList"

        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Cookie": "qgqp_b_id=cd010fed25d96e9edd881a9bffff6edf; HAList=a-sz-300115-%u957F%u76C8%u7CBE%u5BC6; em-quote-version=topspeed; intellpositionL=478px; intellpositionT=1988px; st_si=00804062029990; st_asi=delete; st_pvi=51855661241656; st_sp=2021-09-18%2012%3A44%3A58; st_inirUrl=http%3A%2F%2Fdata.eastmoney.com%2Freport%2Fstock.jshtml; st_sn=5; st_psi=20211001184508305-118000300905-7325285760",
            "Host": "searchapi.eastmoney.com",
            "Referer": "https://so.eastmoney.com/news/s?keyword=000001",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3877.400 QQBrowser/10.8.4507.400",
        }

        # 生成16位随机数字
        random_number_list = [random.randint(0, 9) for i in range(16)]
        random_number_str = ""
        for random_number in random_number_list:
            random_number_str += str(random_number)

        new_list = []
        url_list = []
        title_list = []
        time_list = []
        f = open('../../Desktop/east_money/{}.csv'.format(ts_code), 'w', encoding='utf-8')
        writer = csv.writer(f)
        writer.writerow(['Title', 'CreateTime', 'Content'])
        f.close()
        for i in range(1):
            params_data = {
                "cb": "jQuery3510" + random_number_str + "_" + time_str,
                "keyword": "{}".format(ts_code),  # 要查询的股票代码
                "type": "8193",  # 类型编码
                "pageindex": "{}".format(i),  # 页码
                "pagesize": "25",  # 每页返回的数据量，不建议更改
                "name": "web",  # 模式
                "_": str(int(time.time() * 1000)),  # 生成13位时间戳
            }

            response = requests.get(url, params=params_data, headers=headers)

            # 正则解析提取jQuery中的json数据
            json_data = json.loads(re.sub("jQuery[0-9]*_[0-9]*\(", " ", response.content.decode())[:-1])

            # 遍历数据
            for data in json_data['Data']:
                title_list.append(data['Art_Title'])
                time_list.append(data['Art_CreateTime'].split(' ', 2)[0].replace('-', ''))
                new_list.append({
                    'Title': data['Art_Title'],  # 文章标题
                    'Url': data['Art_UniqueUrl'],  # 文章url
                    'CreateTime': data['Art_CreateTime']  # 发布时间
                })
                url_list.append(data['Art_UniqueUrl'])
            f = 0
            k = 0
            for new_url in url_list:
                target = new_url
                req = requests.get(target)
                req.encoding = 'utf-8'
                html = req.text
                bf = BeautifulSoup(html, 'lxml')
                for x in bf.find_all('div', "txtinfos"):
                    content = re.sub(r'<[^>]+>', '', str(x.text)).replace(' ', '').strip()
                    date = pandas.DataFrame(
                        {'Title': [title_list[f]], 'CreateTime': [time_list[k]], 'content': [content]})
                    date.to_csv('../../Desktop/east_money/{}.csv'.format(ts_code), mode='a+', index=False, header=False)
                    f += 1
                    k += 1

    def start_update(self):
        # self.q()
        self.net()
        self.market()
        # self.update_ma_daily(num = 5)
        # self.update_ma_daily(num = 60)
        # self.update_ma_delta(num1=5, num2=60)
        # for symbol in self.symbols():
        #     self.Dfnews(symbol)


if __name__ == '__main__':
    prime = Handle()
    prime.start_update()
    sched = BlockingScheduler()
    sched.add_job(Handle().start_update, 'cron', day_of_week='mon-sun', hour=19, minute=1, )
    sched.start()



