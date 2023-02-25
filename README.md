# Automated-algorithmic-trading-system
data source: (https://tushare.pro/)
These are some sample files of the system, becuase of the regulations of the company, I am only allow to show partial work I did.

1.update_ma.py:
is the algorithm calculating the 5 days, 10 days moving average of a chosen stock, where the stock data is stored in the MySql database. 

2.update_20.py:
contains 


(1)A web crawler, pulled textual data (daily news of the company) from eastmoney.com . 
(2)A report generator, generate the finalized result into an excel xlsx file.

3.update_news:
Data crawler which pull textual data of daily news from sina.com

4.update_complete_dt:

(1)A ETL process, pulling data from data source, updating the daily stock info into our Mysql database, and calculate rates using existing data in Mysql DB with Pysql and Pandas dataframe

(2) Transform process: Pulling data from SQL, and calculate result using python Pandas, Generate an excel sheet which contains a list of stocks based discussed tech-trading algorithm, example of factors: FCFF,net income, 5 days moving average of close price, etc.
