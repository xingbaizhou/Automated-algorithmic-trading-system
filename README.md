# Automated-algorithmic-trading-system
data source: (https://tushare.pro/)

1.update_ma.py:
is the algorithm calculating the 5 days, 10 days moving average of a chosen stock, where the stock data is stored in the MySql database. 

2.update_20.py:
contains 

(1)A ETL process, pulling data from data source, updating the daily stock info into our Mysql database, and calculate rates using existing data in Mysql DB with Pysql and Pandas dataframe

(2)A web crawler, pulled textual data (daily news of the company) from eastmoney.com . 
(3)A report generator, generate the finalized result into an excel xlsx file.

3.update_news:
Data crawler which pull textual data of daily news from sina.com

