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
import os

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
            data = pro.daily(ts_code='689009.sh', start_date='20210929', end_date='20211011')
            print(data)
            if not data.empty:
                pd.io.sql.to_sql(data, '689009_SH', self.engine, schema='zjkj', if_exists='append',
                                    index=False)
        except Exception as e:
                    print(e)

    def net(self):
        """
        资金流向
        :return:
        """
        list5 = ['689009.sh']
        for i in range(0, len(list5)):
            list2 = []
            try:
                self.engine.execute(
                    'alter table {} add inflow float default null'.format(self.list1[i].replace('.', '_').lower()))
            except Exception as e:
                print(e)
            finally:
                try:
                    df = pro.moneyflow(ts_code='{}'.format(self.list1[i]), start_date='20210929', end_date='20211011')
                    for k in df.net_mf_amount:
                        list2.append(k)
                    o = 0
                    for j in df.trade_date:
                        print(self.list1[i])
                        self.engine.execute('update {} set inflow = {} where trade_date = {}'.format(
                            self.list1[i].replace('.', '_').lower(), list2[o], j))
                        o += 1
                except Exception as x:
                    print(x)

    def market(self):
        """
        流通市值
        :return:
        """
        list5 = ['689009.sh']
        for i in range(0, len(list5)):
            list2 = []
            try:
                self.engine.execute('alter table {} add market_equity float(100,8) default null'.format(
                    self.list1[i].replace('.', '_').lower()))
            except Exception as e:
                print(e)
            finally:
                try:
                    df = pro.daily_basic(ts_code='{}'.format(self.list1[i]), start_date='20210929', end_date='20211011')
                    for k in df.circ_mv:
                        list2.append(k)
                    o = 0
                    for j in df.trade_date:
                        print(self.list1[i])
                        self.engine.execute('update {} set market_equity = {} where trade_date = {}'.format(
                            self.list1[i].replace('.', '_').lower(), list2[o], j))
                        o += 1
                except Exception as x:
                    print(x)

    def update_ma_daily(self, num2):
        list5 = ['689009.sh']
        for stock in list5:
            try:
                self.engine.execute("alter table {} add ma5 double".format(stock), con=self.conn)
                self.engine.execute("alter table {} add ma60 double".format(stock), con=self.conn)
                self.engine.execute("alter table {} add ma_5_60_delta double".format(stock), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    stock_data = pd.read_sql("select * from (select * from {} order by trade_date desc limit {}) t1 order by t1.trade_date".format(stock,num2+90), index_col='trade_date',
                                             parse_dates='trade_date', con=self.conn)
                    ma_update = stock_data['close'].rolling(num2).mean()
                    print(ma_update)
                    for row in ma_update.items():
                        if not isnan(row[1]):
                            trade_date = row[0].strftime("%Y%m%d")
                            value = row[1]
                            self.engine.execute(
                                "update {} set ma{} = {} where trade_date = {}".format(stock, num2, value, trade_date),conn =self.conn)
                except Exception as e:
                    print(e)

    def update_ma_delta(self, num1, num2):
        list5 = ['689009.sh']
        for stock in list5:
            try:
                self.engine.execute("alter table {} add ma_{}_{}_delta double".format(stock,num1,num2), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    self.engine.execute(
                        "update {} set ma_{}_{}_delta = (ma{}-ma{})/ma{} where trade_date between {} and {}".format(stock, num1, num2,
                                                                                                       num1, num2, num2,
                                                                                                          '20210711','20211011'),
                        con=self.conn)
                except Exception as e:
                    print(e)

    def Dfnews(self, ts_code):
        time_str = str(int(time.time() * 1000))
        time.sleep(0.5)

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
        if not os.path.exists('../../Desktop/eastmoney/{}'.format(ts_code)):
            f = open('../../Desktop/eastmoney/{}'.format(ts_code), 'w', encoding='utf-8')
            writer = csv.writer(f)
            writer.writerow(['Title', 'CreateTime', 'Content'])
            f.close()
        for i in range(1):
            params_data = {
                "cb": "jQuery3510" + random_number_str + "_" + time_str,
                "keyword": "{}".format(ts_code),  # 要查询的股票代码
                "type": "8193",  # 类型编码
                "pageindex": "{}".format(i),  # 页码
                "pagesize": "18",  # 每页返回的数据量，不建议更改
                "name": "web",  # 模式
                "_": str(int(time.time() * 1000)),  # 生成13位时间戳
            }

            response = requests.get(url, params=params_data, headers=headers)

            # 正则解析提取jQuery中的json数据
            json_data = json.loads(re.sub("jQuery[0-9]*_[0-9]*\(", " ", response.content.decode())[:-1])

            # 遍历数据
            try:
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
                        date.to_csv('../../Desktop/eastmoney/{}'.format(ts_code), mode='a+', index=False, header=False)
                        f += 1
                        k += 1
            except Exception as e:
                print(e)

    def get_stock_list(self):
        """

        :return: list of all stocks
        """
        stock = self.engine.execute("show tables").fetchall()
        stock = stock[:-12]
        stock = stock[1:]
        new_list = []
        for s in stock:
            new_list.append(s[0])
        return new_list

    def inflows_over_market_equity_rank(self, write_csv=False, write_json=False):
        """
        return the inflow/market equity value of the last three days.
        :param ts_code:
        :return:
        """
        now = datetime.date.today()
        today = str(now) + '资金流入:流通市值,14天正,前五天'
        stock_list = self.get_stock_list()
        if write_csv:
            f = open('../../Desktop/data_report/inflow_over_market_equity/{}'.format(today), 'w', encoding='utf-8')
            csv_writer = csv.writer(f)
            csv_writer.writerow(['股票代码', '五天资金流入', '五天流通市值', '资金/市值'])
        res_list = []
        for s in stock_list:
            f_d = self.engine.execute("select sum(inflow),sum(market_equity) from {} order by trade_date desc limit 14"
                                      .format(s)).fetchall()
            if f_d[0][0] and f_d[0][1]:
                if f_d[0][0] > 0:
                    try:
                        w_d = self.engine.execute(
                            "select sum(inflow),sum(market_equity) from {} order by trade_date desc limit 5"
                                .format(s), con=self.conn).fetchall()
                        ratio = w_d[0][0] / w_d[0][1]

                        if write_csv:
                            csv_writer.writerow([s, w_d[0][0], w_d[0][1], ratio])
                        res_list.append(s)
                    except Exception as e:
                        print(e)
        if write_csv:
            f.close()
        if write_json:
            df = pd.read_csv('../../Desktop/data_report/inflow_over_market_equity/{}'.format(today))
            df.to_json('../../Desktop/data_report_json/inflow_over_market_equity/{}'.format(today), orient='records',
                       force_ascii=False)
        return res_list

    def l3_x_ratio(self, write_csv=False, write_json=False):
        """

        :return:
        """
        #######test########
        self.inflows_over_market_equity_rank(write_csv=True)
        df_c = pd.read_csv("../../Desktop/data_report/inflow_over_market_equity/2021-09-24资金流入:流通市值,14天正,前五天")
        c_list = []
        code_list = []
        for item in df_c.iteritems():
            for code in item:
                c_list.append(code)
            break
        for item in c_list[1]:
            print(item)
            code_list.append(item.replace("_", ".").upper())
        print(code_list)
        ########test end#######

        now = datetime.date.today()
        rank_l3 = pd.read_sql("select * from(select \
               distinct ts_code,con_code,name,area,overall,level,industry_name,index_code \
           from \
               (select con_code,level,industry_name,l3.index_code  from industry_code c1,industry_l3 l3 where c1.index_code = l3.index_code) t1,stock_industries s1 \
           where\
               s1.ts_code = t1.con_code order by ts_code) t1 where 4>(select count(*) from (select \
               distinct ts_code,con_code,name,area,overall,level,industry_name,index_code \
           from \
               (select con_code,level,industry_name,l3.index_code  from industry_code c1,industry_l3 l3 where c1.index_code = l3.index_code) t1,stock_industries s1 \
           where\
               s1.ts_code = t1.con_code order by ts_code) t2 where t1.industry_name = t2.industry_name and t1.overall <= t2.overall) order by industry_name desc,overall desc",
                              con=self.conn)
        rank_res_list = []
        ratio_res_list = []
        for code in rank_l3.ts_code:
            rank_res_list.append(code)
        # r_res_list = self.inflows_over_market_equity_rank()
        # for sk in code_list:
        #     print(sk)
        #     chew = sk.split('_')
        #     print(chew)
        #     when = chew[0] + '.' + chew[1].upper()
        #     ratio_res_list.append(when)

        overall_x_ratio = list(set(rank_res_list).intersection(set(code_list)))
        res_tup = tuple(overall_x_ratio)
        print(res_tup)
        cross_df = pd.read_sql("select ts_code,level,industry_name as level_three,industry,name,area\
           from stock_industries s1,\
           (select distinct con_code,c1.index_code,industry_name,level from industry_code c1,\
           industry_l3 l3 where con_code in {} and c1.index_code = l3.index_code) s2 where ts_code in {} and \
           s1.ts_code = s2.con_code order by industry".format(res_tup, res_tup), con=self.conn)

        if write_csv:
            cross_df.to_csv("../../Desktop/data_report/level_cross_ratio/申万行业分类_L3_{}".format(self.day_str),
                            header=True, index=False)

            f = open("../../Desktop/data_report/level_cross_ratio/ma_chart_{}".format(self.day_str), 'w',
                     encoding='utf-8')
            csv_writer = csv.writer(f)
            csv_writer.writerow(['ts_code', 'ma5', 'ma60', 'ma_5_60_delta'])
            for code in res_tup:
                code = code.replace('.', '_')
                try:
                    ma_df = pd.read_sql(
                        "select ts_code,ma5,ma60,ma_5_60_delta from {} where trade_date = {} order by ts_code".format(
                            code, self.day_str), con=self.conn)
                    print(ma_df)
                    ma_df.to_csv("../../Desktop/data_report/level_cross_ratio/ma_chart_{}".format(self.day_str),
                                 mode='a+',
                                 header=True, index=False)
                except Exception as e:
                    print(e)
            # 读取数据
            r1 = pd.read_csv("../../Desktop/data_report/level_cross_ratio/ma_chart_{}".format(self.day_str))  # 文件1
            r2 = pd.read_csv("../../Desktop/data_report/level_cross_ratio/申万行业分类_L3_{}".format(self.day_str))  # 文件2
            # 数据合并

            all_data_st = pd.merge(r2, r1, how='left', on='ts_code')

            # # 空值填充为0
            # all_data_st['r1_cnt'].fillna(0, inplace=True)
            # all_data_st['r2_cnt'].fillna(0, inplace=True)

            # 导出结果数据
            all_data_st.to_csv("../../Desktop/data_report/level_cross_ratio/final_申万行业分类_L3_{}".format(self.day_str))
        return cross_df

    def life_cycle(self, file_date):
        """

        :return:
        """
        # self.engine.execute("CREATE TABLE life_cycle_stats(ts_code text,sw_level varchar(2),l3_industry text,\
        # head_industry text,company_name text,chg double(20,5),date varchar(8),age int, in_table boolean, in_date varchar(8), out_date varchar(8))",
        #                     con=self.conn)

        f = open('../../Desktop/data_report/archive_list/{}'.format(self.day_str), 'w', encoding='utf-8')
        csv_writer = csv.writer(f)
        # csv_writer.writerow(['股票代码', '申万等级', 'L3_市场', '上级市场', '公司', '涨跌幅', '日期' '累积生命时长', '在表清况', '入表时间', '出表日期'])
        data = pd.read_csv('../../Desktop/data_report/level_cross_ratio/申万行业分类_L3_{}'.format(file_date))

        df_test = pd.read_sql("select * from life_cycle_stats", con=self.conn)
        if df_test.empty:
            for rows in data.iterrows():
                # 1
                ###################
                ts_code = rows[1][0]
                ###################
                table_name = ts_code.replace('.', '_')
                chg_df = pd.read_sql("select pct_chg from {} where trade_date = {}".format(table_name, file_date),
                                     con=self.conn)
                ########################################
                # 2
                sw_level = rows[1][1]
                # 3
                l3_industry = rows[1][2]
                # 4
                head_industry = rows[1][3]
                # 5
                company_name = rows[1][4]
                # 6
                chg = chg_df.pct_chg[0]
                # 7
                life_remain = 5
                # 8
                in_date = file_date
                # 9
                active = 1
                ########################################
                try:
                    self.engine.execute(
                        "INSERT INTO life_cycle_stats VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                            .format(ts_code, sw_level, l3_industry, head_industry, company_name, chg, life_remain,
                                    in_date,
                                    active), con=self.conn)
                except Exception as e:
                    print(e)
        else:
            self.engine.execute("update life_cycle_stats set life_remain = life_remain-1", con=self.conn)
            d_df = pd.read_sql("select * from life_cycle_stats where life_remain = 0", con=self.conn)
            d_df.to_csv("../../Desktop/data_report/archive_list/delete_list/{}".format(file_date), mode='a+')
            self.engine.execute("Delete from life_cycle_stats where life_remain = 0", con=self.conn)
            for rows in data.iterrows():
                # 1
                ###################
                ts_code = rows[1][0]
                df_exist = pd.read_sql("select * from life_cycle_stats where ts_code = '{}'".format(ts_code),
                                       con=self.conn)
                table_name = ts_code.replace('.', '_')
                chg_df = pd.read_sql("select pct_chg from {} where trade_date = {}".format(table_name, file_date),
                                     con=self.conn)

                ########################################
                # 2
                sw_level = rows[1][1]
                # 3
                l3_industry = rows[1][2]
                # 4
                head_industry = rows[1][3]
                # 5
                company_name = rows[1][4]
                # 6
                chg = chg_df.pct_chg[0]
                # 7
                life_remain = 5
                # 8
                in_date = file_date
                # 9
                active = 1
                ########################################
                if df_exist.empty:
                    try:
                        self.engine.execute(
                            "INSERT INTO life_cycle_stats VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                                .format(ts_code, sw_level, l3_industry, head_industry, company_name, chg, life_remain,
                                        in_date, active), con=self.conn)
                    except Exception as e:
                        print(e)

                else:
                    self.engine.execute(
                        "update life_cycle_stats set life_remain = 5 where ts_code ='{}'".format(ts_code),
                        con=self.conn)
                    try:
                        self.engine.execute(
                            "INSERT INTO life_cycle_stats VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                                .format(ts_code, sw_level, l3_industry, head_industry, company_name, chg, life_remain,
                                        in_date, active), con=self.conn)
                    except Exception as e:
                        print(e)

        df_5 = pd.read_sql("select * from life_cycle_stats where life_remain < 5".format(ts_code), con=self.conn)
        table_name = ts_code.replace('.', '_')
        chg_df = pd.read_sql("select pct_chg from {} where trade_date = {}".format(table_name, file_date),
                             con=self.conn)
        for item in df_5.iterrows():
            # 1
            ts_code = item[1][0]
            # 2
            sw_level = item[1][1]
            # 3
            l3_industry = item[1][2]
            # 4
            head_industry = item[1][3]
            # 5
            company_name = item[1][4]
            # 6
            chg = chg_df.pct_chg[0]
            # 7
            life_remain = item[1][6]
            # 8
            in_date = file_date
            # 9
            active = 0

            try:
                self.engine.execute(
                    "INSERT INTO life_cycle_stats VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                        .format(ts_code, sw_level, l3_industry, head_industry, company_name, chg, life_remain, in_date,
                                active), con=self.conn)
            except Exception as e:
                print(e)

    def delAll(self, path):
        """
        delete directory recursively
        :param path:
        :return:
        """
        if os.path.isdir(path):
            files = os.listdir(path)
            for file in files:
                p = os.path.join(path, file)
                if os.path.isdir(p):
                    # recursive
                    self.delAll(p)
                else:
                    os.remove(p)
        else:
            os.remove(path)

    def daily_plot_json(self):
        """

        :return:
        """
        # daily_info
        path = "../../Desktop/data_report_json/daily_info"
        s_list = self.get_stock_list()
        self.delAll(path)
        for s in s_list:
            name = s.split("_")
            stock_name = name[0] + '.' + name[1].upper()
            df_profit = self.fetch_stock_daily_info(s, '20150101', self.day_str)
            df_profit.to_json("../../Desktop/data_report_json/daily_info/{}".format(stock_name),orient='split',
                              force_ascii = False,index = False)

    def pull_sql_stats(self):
        """

        :return:
        """
        # df = pd.read_sql(
        #     "select ts_code,count(*),in_date as consecutive_days from life_cycle_stats where active = 1 group by ts_code,in_date",
        #     con=self.conn)
        df_2 = pd.read_sql("select * from life_cycle_stats order by ts_code", con=self.conn)
        df_2.to_csv("../../Desktop/data_report/archive_list/archive_{}".format(self.day_str),index = False)
        df_2.to_json("../../Desktop/data_report_json/archive_list/archive_{}".format(self.day_str),force_ascii=False,orient = 'split',index = False)
        df_final = pd.read_csv("../../Desktop/data_report/level_cross_ratio/final_申万行业分类_L3_{}".format(self.day_str))
        df_final.to_json("../../Desktop/data_report_json/level_cross_ratio/final_申万行业分类_L3_{}".format(self.day_str),orient = 'split',force_ascii=False,index=False)

    def start_update(self):
        #self.q()
        #self.net()
        #self.market()
        self.update_ma_daily(num2 = 5)
        #self.update_ma_daily(num2 = 60)
        self.update_ma_delta(num1=5, num2=60)
        # self.l3_x_ratio(write_csv=False, write_json=True)
        # self.life_cycle(self.day_str)
        # self.pull_sql_stats()
        # self.daily_plot_json()

    def update_news(self):
        update_list = self.symbols[2239:]
        for symbol in update_list:
            self.Dfnews(symbol)


if __name__ == '__main__':
    prime = Handle()
    sched = BlockingScheduler()
    prime.start_update()
    # sched.add_job(Handle().start_update, 'cron', day_of_week='mon-sun', hour=19, minute=1, )
    # sched.add_job(Handle().update_news, 'cron', day_of_week='mon-sun', hour=23, minute=0, )
    #sched.start()



