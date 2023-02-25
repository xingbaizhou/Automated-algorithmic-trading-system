import datetime
import sqlalchemy
import pymysql
import requests
import time
import json
import random
import re
from bs4 import BeautifulSoup
import pandas
import csv


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
    f = open('../../Desktop/east_money/{}'.format(ts_code), 'w', encoding='utf-8')
    writer = csv.writer(f)
    writer.writerow(['Title', 'Date', 'Content'])
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
                date = pandas.DataFrame({'Title': [title_list[f]], 'CreateTime': [time_list[k]], 'content': [content]})
                date.to_csv('../../Desktop/east_money/{}'.format(ts_code), mode='a+', index=False, header=False)
                f += 1
                k += 1


if __name__ == '__main__':
    db_url = 'mysql+pymysql://root:admin@192.168.2.175/zjkj'
    day_str = datetime.datetime.now().strftime("%Y%m%d")
    # 入库
    engine = sqlalchemy.create_engine(db_url)
    # 连接数据库 取出每支股票代码
    conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                           charset='utf8')
    stock = engine.execute("show tables").fetchall()
    stock = stock[:-12]
    stock = stock[1:]
    stock_list = []
    for s in stock:
        p = s[0].split('_')
        stock_list.append(p[0])
    print(stock_list)
    for code in stock_list:
        Dfnews(code)
