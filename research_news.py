import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import csv
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3877.400 QQBrowser/10.8.4507.400"}


def article(url):
    """
    提取文章详情
    :param url: 文章链接
    :return: 文章内容
    """

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # 新闻内容
    new_content = soup.find(class_="newsContent")
    if not new_content:
        return ''
    return new_content.text



def scrapy():
    """
    主程序
    :return:
    """
    today = datetime.datetime.now().strftime('%Y%m%d')
    file_name = 'news_info'
    stockCode_list = []
    stockName_list = []
    Date_list = []
    title_list = []
    new_content_list = []
    #write title
    # f = open('../../Desktop/data_report/research_news/{}'.format(file_name), 'w', encoding='utf-8')
    # csv_writer = csv.writer(f)
    # csv_writer.writerow(['symbol', 'name', 'date', 'title', 'content'])
    # f.close()
    for i in range(1,1+1):  # 表示从 第1页 提取至 第10页
        print(i)
        parameters = {
            "cb":"datatable9020696",
            "industryCode":"*",
            "pageSize":"50",
            "industry":"*",
            "rating":"",
            "ratingChange":"",
            "beginTime":today,
            "endTime":today,
            "pageNo":i,
            "fields":"",
            "qType":"0",
            "orgCode":"",
            "code":"*",
            "rcode":"",
            "p":i,
            "pageNum":i,
            "pageNumber":i,
            "_":round(time.time()),  # 生成13位时间戳
            }
        time.sleep(1)

        response = requests.get("http://reportapi.eastmoney.com/report/list?", params=parameters, headers=headers)

        try:
            for shares in eval(response.content.decode()[17:-1])["data"]:
                # 股票代码
                stockCode = shares["stockCode"]
                # 股票简称
                stockName = shares["stockName"]
                # 日期
                publishDate = shares["publishDate"]
                # 报告名称
                title = shares["title"]
                # 报告文章链接
                title_url = f"http://data.eastmoney.com/report/zw_stock.jshtml?infocode={shares['infoCode']}"
                # 文章内容
                new_content = article(title_url)
                stockCode_list.append(stockCode)
                stockName_list.append(stockName)
                Date_list.append(publishDate.split(' ',2)[0].replace('-',''))
                title_list.append(title)
                new_content_list.append(new_content)
        except Exception as e:
            print(e)

    data = pd.DataFrame({'StockCode':stockCode_list,'StockName':stockName_list,'Date':Date_list,'Title':title_list,'Content':new_content_list})
    data.to_csv('../../Desktop/data_report/research_news/{}'.format(file_name),mode='a+',index=False,header=False)


def csv_to_json():
    """

    :return:
    """
    try:
        file_name = 'news_info'
        df_csv = pd.read_csv('../../Desktop/data_report/research_news/{}'.format(file_name))
        df_csv.to_json('../../Desktop/data_report_json/research_news/{}'.format(file_name), orient='records',force_ascii=False)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    sched = BlockingScheduler()
    sched.add_job(scrapy, 'cron', day_of_week='mon-sun', hour=16, minute=55, )
    sched.add_job(csv_to_json,'cron',day_of_week='mon-sun', hour=17, minute=00,)
    sched.start()
