import pymysql
from sqlalchemy import create_engine
from math import isnan
import pandas as pd
import datetime


class MA:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:admin@192.168.2.175/zjkj')
        self.conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                                    charset='utf8')
        self.day_str = datetime.datetime.now().strftime("%Y%m%d")
        stock = self.engine.execute("show tables").fetchall()
        stock = stock[:-12]
        stock = stock[1:]
        self.stock_list = []
        for s in stock:
            self.stock_list.append(s[0])

    def update_ma(self, num):
        """

        :return:
        """
        count = 0
        for stock in self.stock_list:
            try:
                self.engine.execute("alter table {} add ma{} double".format(stock, num), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    stock_data = pd.read_sql(
                        "select * from {} where trade_date between '20210825' and '20210930' order by trade_date asc".format(
                            stock), con=self.conn,
                        index_col='trade_date',
                        parse_dates='trade_date')
                    ma5 = stock_data['close'].rolling(num).mean()
                    print(stock, count)
                    count += 1
                    for row in ma5.items():
                        print(row)
                        if not isnan(row[1]):
                            trade_date = row[0].strftime("%Y%m%d")
                            value = row[1]
                            self.engine.execute(
                                "update {} set ma{} = {} where trade_date = {}".format(stock, num, value, trade_date))
                except Exception as e:
                    print(e)

    def update_ma_daily(self, num2):
        for stock in self.stock_list:
            try:
                self.engine.execute("alter table {} add ma{} double".format(stock, num2), con=self.conn)

            except Exception as e:
                print(e)
            finally:
                try:
                    stock_data = pd.read_sql("select * from '000001_sz' order by trade_date desc limit {}".format(num2 + 1),
                                             con=self.conn)
                    ma_update = stock_data['close'].rolling(num2).mean()
                    for row in ma_update.items():
                        if not isnan(row[1]):
                            trade_date = row[0].strftime("%Y%m%d")
                            value = row[1]
                            self.engine.execute(
                                "update {} set ma{} = {} where trade_date = {} limit {}".format(stock, num2, value,
                                                                                                trade_date,
                                                                                                self.day_str, num2))
                except Exception as e:
                    print(e)

    def update_ma_delta(self, num1, num2):
        for stock in self.stock_list:
            try:
                self.engine.execute("alter table {} add ma_{}_{}_delta double".format(stock, num1, num2), con=self.conn)
            except Exception as e:
                print(e)
            finally:
                try:
                    self.engine.execute(
                        "update {} set ma_{}_{}_delta = (ma{}-ma{})/ma{} where trade_date = {}".format(stock, num1,
                                                                                                       num2,
                                                                                                       num1, num2, num2,
                                                                                                       self.day_str),
                        con=self.conn)
                except Exception as e:
                    print(e)

    def start_update(self):
        self.update_ma_daily(num2=5)
        # self.update_ma_delta(num1=12, num2=60)


if __name__ == "__main__":
    a = MA()
    a.start_update()
