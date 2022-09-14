import os
import pandas as pd
import numpy as np
import pymysql


def connect_and_fetch_data(IMEI_number,start_time,end_time):

     # print ("connect database and fetch data......")
     print ("正在连接数据库获取数据中......")
     #连接数据库
     db = pymysql.connect(
     host='gz-cdb-9avl6ee9.sql.tencentcdb.com',
     port=57122,
     user='read',
     passwd='feier222X',
     charset='utf8'
     )

     #创建游标
     cursor = db.cursor()

     #查看数据库
     cursor.execute("show databases")

     #接收查询结果
     DBs = cursor.fetchall() #返回的数据结果是元组
     # print("databases are:",DBs)


     #进入water数据库
     cursor.execute("use water")
     #查看数据表
     cursor.execute("show tables")

     #查看数据表
     water_tables = cursor.fetchall()
     # print("tables are:",water_tables)


     part_table_name = 'water_data_'
     # IMEI_number = 'FA78EFAFF9D0'
     table_name = part_table_name + IMEI_number

     # start_time = 1661868121
     # end_time = 1661954521
     #to do prams
     #查询语句


     sql = 'select * from ' + table_name + ' where time >= %s and time < %s' 
     # print("sql ----",sql)
     args = start_time,end_time

     # sql = "select * from water_data_FA78EFAFF9D0 where time >= 1661868121 and time <1661954521"


     #存储文件名和路径
     # path_csv_filename = '/Users/qinjianquan/Desktop/tmpfile/water_data_FA78EFAFF9D0.csv'
     # path_excel_filename = '/Users/qinjianquan/Desktop/tmpfile/water_data_FA78EFAFF9D0.xlsx'


     #执行查询并返回结果
     cursor.execute(sql,args)
     #返回查询到的数据
     rows = cursor.fetchall()

     #转换数据对象
     data_table = pd.DataFrame(list(rows),columns = ['时间', 'ut', 'dn', 'i', '心率', '低压', '高压', '前面积', '后面积', 'RR', 'step', 'acc_x', 'acc_z', 'acc_y'] )
     # print("raw_data_table:",data_table)
     print("数据已经成功获取！")

     print("正在整理数据中...")

     #重命名
     df_csv =data_table

     #整理数据，计算体动
     df_csv["add_xyz"] = df_csv["acc_x"] + df_csv["acc_z"] + df_csv["acc_y"]

     df_csv["sub_xyz"] = df_csv["add_xyz"] - df_csv["add_xyz"].shift(-1)

     df_csv["dsub_xyz"] = df_csv["sub_xyz"] - df_csv["sub_xyz"].shift(-1)

     df_csv["dsub_xyz/10"] = df_csv["dsub_xyz"].div(10)
     df_csv["round"] = df_csv["dsub_xyz/10"].round()
     df_csv["abs"] = df_csv["round"].abs()
     df_csv["abs_52"] = df_csv["abs"]

     df_csv.loc[df_csv["abs"] > 27,'abs_52'] = df_csv["abs_52"].sub(27)
     df_csv["体动"] = df_csv["abs_52"]

     df_csv.loc[df_csv["体动"] > 10000,'体动'] = df_csv["体动"].div(10)
     df_csv.loc[df_csv["体动"] > 10000,'体动'] = df_csv["体动"].div(10)

     # print("computed file:",df_csv)

     #数据类型转换
     df_csv['时间']=pd.to_datetime(df_csv['时间'],unit='s',origin=pd.Timestamp('1970-01-01'))

     #删除无效行
     df_csv.drop(df_csv.tail(2).index,inplace=True)

     #删除多余列
     df_csv=df_csv.drop(["abs","abs_52","round","ut","dn","i","step","acc_x","acc_y","acc_z","add_xyz","sub_xyz","dsub_xyz","dsub_xyz/10"],axis=1)

     #保存文件
     # path_data_filename = '/Users/qinjianquan/Desktop/FEIER/GCYY/raw_data.csv'
     path_data_filename = output = 'static/csv/' + "raw_data" + '.csv'
     # path_data_filename = "raw_data" + '.csv'
     df_csv.to_csv(path_data_filename,index=False,header=True) 

     # print("well, data has been fetched and saved as raw_data.csv......")
     print("已完成数据整理,☑️")
     #执行
     # str = ('python3 /Users/qinjianquan/Desktop/FEIER/GCYY/analyze_model.py')
     str = ('python3 python_gcyy/analyze_model.py')
     p= os.system(str)

     print("正在建模分析中....",p)




