import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import requests
import re
from bs4 import BeautifulSoup
import sqlalchemy
import pymysql
import pandas

class Hot_news(object):
    def __init__(self):
        self.url_list = []
        self.list0 = []
        self.list1 = []
        self.list2 = []
        self.list3 = []
        self.list4 = []
        self.list5 = []
        self.list6 = []
        self.list7 = []
        self.list8 = []
        db_url = 'mysql+pymysql://root:admin@192.168.2.175/zjkj'
        self.conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                               charset='utf8')
        # 入库
        self.engine = sqlalchemy.create_engine(db_url)
        target = 'http://stock.eastmoney.com/'
        req = requests.get(target)  # 获取对象
        req.encoding = 'utf-8'
        html = req.text  # 获得网页源代码
        bf = BeautifulSoup(html, 'lxml')  # 利用BeautifulSoup进行解析
        for i in bf.find_all('ul',"common_list list-md")[0]:
            for x in i.find_all('a'):
                a = x.get('href')
                self.url_list.append(a)
        print(self.url_list)

    def handle(self):
        target = '{}'.format(self.url_list[0])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for i in bf.find_all('div',"item")[0]:
            self.date = i.split(' ',2)[0].replace('年','').replace('月','').replace('日','')
            if self.date.isdigit():
                print(self.date)
            else:
                for i in bf.find_all('div',"time")[0]:
                    self.date = i.split(' ',2)[0].replace('年','').replace('月','').replace('日','')
                    print(self.date)
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第一篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list8:
                                self.list0.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents1 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第一篇热点新闻:{}'.format(self.contents1))

            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list0:
                            self.list0.append(symbol)
    def handle1(self):
        target = '{}'.format(self.url_list[1])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第二篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            print(p)
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list1:
                                self.list1.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents2 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第二篇热点新闻:{}'.format(self.contents2))
            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list1:
                            self.list1.append(symbol)

    def handle2(self):
        target = '{}'.format(self.url_list[2])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第三篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list2:
                                self.list2.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents3 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第三篇热点新闻:{}'.format(self.contents3))
            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list2:
                            self.list2.append(symbol)

    def handle3(self):
        target = '{}'.format(self.url_list[3])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第四篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list3:
                                self.list3.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents4 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第四篇热点新闻:{}'.format(self.contents4))
            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list3:
                            self.list3.append(symbol)

    def handle4(self):
        target = '{}'.format(self.url_list[4])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第五篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list4:
                                self.list4.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents5 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第五篇热点新闻:{}'.format(self.contents5))
            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list4:
                            self.list4.append(symbol)

    def handle5(self):
        target = '{}'.format(self.url_list[5])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第六篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list5:
                                self.list5.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents6 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第六篇热点新闻:{}'.format(self.contents6))

            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list5:
                            self.list5.append(symbol)

    def handle6(self):
        target = '{}'.format(self.url_list[6])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第七篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list6:
                                self.list6.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents7 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第七篇热点新闻:{}'.format(self.contents7))

            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list6:
                            self.list6.append(symbol)

    def handle7(self):
        target = '{}'.format(self.url_list[7])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第八篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list7:
                                self.list7.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents8 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第八篇热点新闻:{}'.format(self.contents8))

            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list7:
                            self.list7.append(symbol)

    def handle8(self):
        target = '{}'.format(self.url_list[8])
        req = requests.get(target)
        req.encoding = 'utf-8'
        html = req.text
        bf = BeautifulSoup(html, 'lxml')
        for s in bf.find_all('div', "left-content"):
            for i in s.find_all('div', "Body"):
                contents = re.sub(r'<[^>]+>', '', str(i)).replace(' ', '').strip()
                print('第九篇热点新闻:{}'.format(contents))
                for z in i.find_all('a'):
                    d = z.get('href')
                    # print(d)
                    result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                    for q in result:
                        if q != '[]':
                            p = q
                            last_result = re.findall('\d{6}', p)
                            symbol = str(last_result).replace('[', '').replace(']', '').replace("'", '')
                            if symbol not in self.list8:
                                self.list8.append(symbol)
        for j in bf.find_all('div',"txtinfos"):
            self.contents9 = re.sub(r'<[^>]+>', '', str(j)).replace('\n', '').strip()
            print('第九篇热点新闻:{}'.format(self.contents9))
            for z in j.find_all('a'):
                d = z.get('href')
                # print(d)
                result = re.findall('http://quote.eastmoney.com/.*\d{6}', d)
                for q in result:
                    if q != '[]':
                        p = q
                        last_result = re.findall('\d{6}',p)
                        symbol = str(last_result).replace('[','').replace(']','').replace("'",'')
                        if symbol not in self.list8:
                            self.list8.append(symbol)

    def handle_all(self):
        result = self.list0 + self.list1 + self.list2 + self.list3 + self.list4 + self.list5 + self.list6 +self.list7 + self.list8
        l = [str(a) for a in self.list0]
        list0 = ','.join(l)

        l = [str(a) for a in self.list1]
        list1 = ','.join(l)

        l = [str(a) for a in self.list2]
        list2 = ','.join(l)

        l = [str(a) for a in self.list3]
        list3 = ','.join(l)

        l = [str(a) for a in self.list4]
        list9 = ','.join(l)

        l = [str(a) for a in self.list5]
        list5 = ','.join(l)

        l = [str(a) for a in self.list6]
        list6 = ','.join(l)

        l = [str(a) for a in self.list7]
        list7 = ','.join(l)
        l = [str(a) for a in self.list8]
        list8 = ','.join(l)
        list4 = []
        list4.append(list0)
        list4.append(list1)
        list4.append(list2)
        list4.append(list3)
        list4.append(list9)
        list4.append(list5)
        list4.append(list6)
        list4.append(list7)
        list4.append(list8)
        print(list4)

        self.code_dict = {}
        for i in result:
            self.code_dict[i] = result.count(i)
        print(self.code_dict)
        aaa = self.engine.execute('select date from hot_news where 1=1 order by date desc limit 1').fetchall()
        date = str(aaa).split("'", 3)[1]
        if self.date != date:
            a = 0
            for u in self.url_list:
                print(self.date)
                try:
                    self.engine.execute('insert into hot_news(url,date,ts_code) value("{}","{}","{}")'.format(u,self.date,list4[a]))
                    a += 1
                except Exception as e:
                    print(e)
    def start(self):
        self.handle()
        self.handle1()
        self.handle2()
        self.handle3()
        self.handle4()
        self.handle5()
        self.handle6()
        self.handle7()
        self.handle8()
        self.handle_all()

if __name__ == '__main__':
    #Hot_news().start()
    sched = BlockingScheduler()
    sched.add_job(Hot_news().start, 'cron', day_of_week='mon-sun', hour=23, minute=00,)
    sched.start()