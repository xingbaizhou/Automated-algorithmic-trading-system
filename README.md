# Automated-algorithmic-trading-system
data source: (https://tushare.pro/)

These are some sample files of the system, becuase of the regulations of the company, I am only allow to show partial work I did.


Readme:

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

My duties on the project:

1.Conducted Chinese stock market research and data mining according to business requirements

2.Extracted, cleaned, and organized essential data from companies’ annual report PDFs by Python into Array formats 

3.Built database with SQL under Linux environment with pre-determined relation schema

4.Utilized API provided by different financial institutions to gather China Stock market essential data

5.Programmed web crawler with Python BeautifulSoup package to collect daily news of a total of 5000 companies in the A-share market from various financial news sites

6.Developed quantitative methods of data, stock screening strategies with group members

7.	Performed quantitative analysis on numerical data by SQL queries and Python Panada package


8.	Conducted emotional test by NLP provided by BAIDU AI on news-related data of every company 

9.	Pre-processed finalized data with Python Matplotlib for raw visualization, robotized, debugged and unit tested the program, the program has been continuously compiling and outputting results with no bugs. 

10.	Finalize all data into JSON, and XLTX formats to interface with front-end developers for formal data visualization. Presented final data report daily




