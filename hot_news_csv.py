import sqlalchemy
import pymysql
import pandas




def handle(start_date,end_date,file_name):
    db_url = 'mysql+pymysql://root:admin@192.168.2.175/zjkj'
    conn = pymysql.connect(host='192.168.2.175', port=3306, user='root', password='admin', database='zjkj',
                                charset='utf8')
    # 入库
    engine = sqlalchemy.create_engine(db_url)
    list2 = []
    list1 = []
    data = pandas.read_sql('select * from hot_news where date between {} and {} order by date'.format(start_date,end_date),con=conn)
    for i in data.iterrows():
       list1 += i[1][2].split(",")

       # list2.append(i[1][2].split(","))

    # print(len(list1))
    # print(list2)
    # for x in list2:
    #     print(x)
    code = {}
    for value in list1:
        code[value] = list1.count(value)
    list3 = []
    for ts_code in code.keys():
        list3.append(ts_code)
    print(list3)
    list4 = []
    for count in code.values():
        list4.append(count)
    print(list4)
    for q in list1:
        info = pandas.DataFrame({'ts_code':[q],'count':[code[q]]})
        info.to_csv("~/Desktop/{}ts_code_count.csv".format(start_date + '-' + end_date+file_name),index=False,mode='a+',header=False)



handle('20210918','20210923')