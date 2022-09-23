import imp
import pandas as pd
import numpy as np
import pymysql
import os
import scipy
from scipy import stats
import math
import time
import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine
import json
import requests


def connect_and_fetch_data(IMEI_number,start_time,end_time):

     
     print()
     print ("报告分析中🤔🤔🤔")
     print("------------------------------------------------")

     print ("正在连接数据库获取数据")

     db = pymysql.connect(
     host='gz-cdb-9avl6ee9.sql.tencentcdb.com',
     port=57122,
     user='read',
     passwd='feier222X',
     charset='utf8'
     )

     # 创建游标
     cursor = db.cursor()

     # 进入water数据库
     cursor.execute("use water")

     # 查看数据表
     cursor.execute("show tables")

     # 查看数据表
     water_tables = cursor.fetchall()

     # 用户数据表名
     part_table_name = 'water_data_'
     table_name = part_table_name + IMEI_number

     # 查询条件（获取时间戳）
     latest_timestamp_query = 'SELECT * FROM `water`.`water_data_end_time` WHERE `sn` = "'+ IMEI_number +'" LIMIT 0,1'

     # print("查询数据表最近更新时间语句:",latest_timestamp_query)

     cursor.execute(latest_timestamp_query)

     data_back = cursor.fetchall()
     # print("获取到表最后更新的数据是:",data_back)

     # 转换数据对象
     df_timestamp = pd.DataFrame(list(data_back))
     if df_timestamp.empty:
          print("未获取到任何数据,数据对象为空")
          return True

     else:
          print("截止时间戳已经成功获取✅")
          # print("获取到的第一行数据表为:",df_timestamp)

     end_time = int(df_timestamp.iloc[[0],[1]].values[0][0])

     start_time = end_time - 60 * 60 * 6 

     # print("报告开始时间和结束时间:",start_time,end_time)

     local_end_time = time.localtime(end_time)
     local_start_time = time.localtime(start_time)
     shanghai_end_time = time.strftime("%Y-%m-%d %H:%M:%S",local_end_time)
     shanghai_start_time = time.strftime("%Y-%m-%d %H:%M:%S",local_start_time)

     
     print("报告开始时间:",shanghai_start_time)
     print("报告结束时间:",shanghai_end_time)


     # 截取数据
     sql_by_time = 'select * from ' + table_name + ' where time >= %s and time < %s' 
     args_by_time = start_time,end_time

     # print("查询数据表某段时间数据的语句:",sql_by_time)

     # 执行查询并返回结果
     cursor.execute(sql_by_time,args_by_time)

     #返回查询到的数据
     rows_by_time = cursor.fetchall()
     # print("result:", rows_by_time)

     #使用pandas转换数据对象
     data_table_second_query = pd.DataFrame(list(rows_by_time),columns = ['时间', 'ut', 'dn', 'i', '心率', '低压', '高压', '前面积', '后面积', 'RR', 'step', 'acc_x', 'acc_z', 'acc_y'] )
    
     if data_table_second_query.empty:
          print("Sorry,未获取到任何数据,数据对象为空")
          return True

     else:
          print("数据已经成功获取✅")
          # print("data_table_second_query:",data_table_second_query)

     #重命名
     df_csv =data_table_second_query

     print("正在计算体动")
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
     
     print("体动计算已完成✅")


     #数据类型转换
     df_csv['时间']=pd.to_datetime(df_csv['时间'].values,utc=True,unit='s').tz_convert("Asia/Shanghai")
     
     # print("时间转化后的数据:",df_csv)

     #删除无效行
     df_csv.drop(df_csv.tail(2).index,inplace=True)

     #删除多余列
     df_csv=df_csv.drop(["abs","abs_52","round","ut","dn","i","step","acc_x","acc_y","acc_z","add_xyz","sub_xyz","dsub_xyz","dsub_xyz/10"],axis=1)

     if df_csv.empty:
          print("整理后的数据对象为空,无法继续分析")
          return True
     else:
          HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
          STATICFILES_DIRS = os.path.join(HERE, 'static/csv/')
          print("正在转换数据对象，进一步分析")


     #重命名数据对象
     df = df_csv
     # print("整理后的源数据:")

     # 调整1，去除体动大于100的数据
     df = df.drop(df[(df['体动'] > 100)].index)
     # print("removed data(体动>100):",df)

     # print("清除异常数据:如血压、心率等于零的数据")
     # temp data cleaning
     # df = df.drop(df[(df['心率'] == 0) | (df['低压'] == 0) | (df['高压'] == 0)].index)
     # data clean, replace 0 by  1
     df.replace(to_replace = 0, value = 1, inplace=True)
     df = df.drop(columns=['X', 'Y', 'Z', '前面积','后面积','体动'], errors='ignore')
     # print("丢弃不必要的数据列")
     df = df.drop(df[(df['心率'] == 1) | (df['低压'] == 1) | (df['高压'] == 1) | (df['RR'] == 1)].index)
     # print("丢弃心率、低压、高压、RR等于1的数据")
     df = df.drop(df[(df['低压'] < 40) | (df['高压'] < 80)].index)
     # print("丢弃低压低于40高压高于80的数据")
     # witoutCol = '低压'
     # df = df.drop(columns=[witoutCol])
     df.reset_index(drop=True, inplace=True)
     # print(df)

     if df.empty:
          print("去除异常数据后，数据对象为空,无法继续分析")
          return True
     else:
          print("分析处理完毕✅")

     # ## 一 计算健康标尺

     # In[29]:


     # Geometric Mean of the column in dataframe

     print("正在使用scipy进行高级处理")
     
     # print(len(df.columns)) 
     ##scipy.stats.gmean(df.iloc[:,1:7],axis=0)
     # 结构时中
     GEOMEAN1 = scipy.stats.gmean(df.iloc[:,1:len(df.columns)],axis=1)
     # print("max ----------",GEOMEAN1)
     
     # M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
     QDMM1 = abs(math.log(max(GEOMEAN1)/min(GEOMEAN1),0.618))


     # print(QDMM1)


     # In[ ]:
     def calculateQDMMByFixedYinYang(df_yang, df_yin):
          QDMM_MAX = QDMM1
          colname_max = ''
          index_max = 0
          # print("yin", df_yin)
          if len(df_yin) > 0:
               df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')		
               for (index, colname) in enumerate(df_yin):
                    # print('df_yang: ', index, colname)
                    if index !=0:
                         df_yin[colname] = 1/df_yin[colname]
                         GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
                         # print(GEOMEAN_temp)
                         # M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
                         QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
                         # print(QDMM_temp, index, colname)
                         if QDMM_temp > QDMM_MAX:
                              QDMM_MAX = QDMM_temp
                              colname_max = colname
                              index_max = index	
                              df_drop = df_temp[colname]


     # In[ ]:
     def calculateQDMM(df_yang, df_yin):
          QDMM_MAX = QDMM1
          colname_max = ''
          index_max = 0
          # print("yin", df_yin)
          if len(df_yin) > 0:
               df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')		
               for (index, colname) in enumerate(df_yang):
                    print('df_yang: ', index, colname)
                    if index !=0:
                         df_temp[colname] = 1/df_temp[colname]
                         GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
                         # print(GEOMEAN_temp)
                         # M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
                         QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
                         # print(QDMM_temp, index, colname)
                         if QDMM_temp > QDMM_MAX:
                              QDMM_MAX = QDMM_temp
                              colname_max = colname
                              index_max = index	
                              df_drop = df_temp[colname]
          else:
               df_temp = df_yang.copy()
               for (index, colname) in enumerate(df_temp):
                    # print('no df_yin: ', index, colname)
                    if index !=0:
                         df_temp[colname] = 1/df_temp[colname]
                         GEOMEAN_temp = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)],axis=1)
                         # print(GEOMEAN_temp)
                         # M个数据组结构时中中最大值与最小值的定量差异（最大值与最小值比值以0.618为底取对数，在取绝对值）
                         QDMM_temp = abs(math.log(max(GEOMEAN_temp)/min(GEOMEAN_temp),0.618))
                         # print(QDMM_temp, index, colname)
                         if QDMM_temp > QDMM_MAX:
                              QDMM_MAX = QDMM_temp
                              colname_max = colname
                              index_max = index	
                              df_drop = df_temp[colname]
          # print(QDMM_temp)
          if (index_max != 0):
               # print(QDMM_MAX, colname_max, index_max)
               df_yang = df_yang.drop([colname_max], axis=1)
               # print(df_yang)
               if len(df_yin) > 0:
                    df_yin  = pd.concat([df_yin, df_drop], axis=1, join='inner')
               else:
                    df_yin = df_drop
               # print(df_yin)
               # calculateQDMM(df_yang, df_yin)
          # else:
          # 	print(QDMM_MAX, colname_max, index_max)
          # 	df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')
          # 	print(df_yang)
          return df_yang, df_yin, QDMM_temp, index_max	


     # ## 二 排除异常数据组

     # In[ ]:


     def dataClean(df_yang, df_yin):
          df_temp = pd.concat([df_yang, df_yin], axis=1, join='inner')
          # print(df_temp)
          # df_temp = df.copy()
          for (index, colname) in enumerate(df_temp):
               if index !=0:
                    # 1.在GCYY数据流中计算每个参数的功能时中
                    GEOMEAN_func_temp = scipy.stats.gmean(df_temp.iloc[:,index],axis=0)
                    # print(GEOMEAN_func_temp)
                    # 2.每个参数纵向除以自己的功能时中
                    df_temp[colname] = df_temp[colname]/GEOMEAN_func_temp
               
          # df_temp = df_temp.append(df_temp.sum(axis=1, numeric_only=True), ignore_index=True)
          dataframe_sum = df_temp.sum(axis=1, numeric_only=True)
          # 3.每个数据组横向计算所有参数的和，称为数据和
          # print(dataframe_sum)
          df_temp["sum"] = dataframe_sum
          
          # 4.计算每个参数横向除以自己的数据和，称为占比
          for (index, colname) in enumerate(df_temp):
               if index !=0 and colname != "sum":
                    df_temp[colname] = df_temp[colname]/df_temp["sum"]
          # print(df)
          # 计算每个数据组横向所有占比的结构时中
          GEOMEAN2 = scipy.stats.gmean(df_temp.iloc[:,1:len(df_temp.columns)-2],axis=1)
          # print(GEOMEAN2)
          # 计算所有结构时中的几何均值GM
          GEOMEAN2_GM = scipy.stats.gmean(GEOMEAN2)
          # print(GEOMEAN2_GM)
          # print(GEOMEAN2_GM)
          # 计算每个结构时中与GM的定量差异，计算相继两个结构时中的定量差异，删除其中定量差异大于0.1610的数据组
          
          arr = np.array(GEOMEAN2)
          # todo: #1 check
          qdmmArray = abs(np.log(arr/GEOMEAN2_GM)/np.log(0.618))
          # print(qdmmArray)
          qdmmDiffArray = np.diff(qdmmArray)
          # print(qdmmDiffArray)
          indexrResult = np.where(abs(qdmmDiffArray) > 0.1610)
          # indexrResult = np.where(abs(np.log(arr/GEOMEAN2_GM)/np.log(0.618)) > 0.1610)
          # print(len(indexrResult[0]))
          # tt = []
          # for i in range(arr.shape[0]-1):		
          # 	# QDMM_temp = abs(math.log(arr[i+1]/arr[i],0.618))
          # 	QDMM_temp = abs(np.log(arr[i+1]/arr[i])/np.log(0.618))
          # 	if (QDMM_temp > 0.1610):
          # 		tt.append(i)
          # 		tt.append(i+1)
                    # np.append(indexrResult[0], i)
                    # np.append(indexrResult[0], i+1)
          # print("tt", len(tt))
          # print(len(indexrResult[0]))
          df_temp.drop(indexrResult[0], axis=0, inplace=True)
          df_yang.drop(indexrResult[0], axis=0, inplace=True)
          df_yin.drop(indexrResult[0], axis=0, inplace=True)
          # df.drop([5,6], axis=0, inplace=True)
          return (df_yang, df_yin)


     # In[ ]:


     # QDMM_MAX = QDMM1
     # df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df, pd.DataFrame([]))
     # print(df_yang)
     # print(df_yin)
     # while index_max != 0:		
     # 	df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df_yang, df_yin)

     # special case for specified yin and yang
     for (index, colname) in enumerate(df):
          # print('df: ', index, colname)
          if index !=0 and colname != 'RR':
               df[colname] = 1/df[colname]
     df_yang = df.drop(['心率','低压','高压','时间'], axis=1, errors='ignore')
     df_yin = df.drop(['RR','时间'], axis=1)

     # print(df_yang)
     # print(df_yin)

     df_yang, df_yin = dataClean(df_yang, df_yin)


     # # 三 计算健康标尺中的非常显著性差异点

     # In[ ]:


     # 再重复以上两个大步骤，直到结构不再变化。
     # TODO: temp remove
     # df_yang, df_yin, QDMM_temp, index_max = calculateQDMM(df_yang, df_yin)
     # print(df_yang)
     # print(df_yin)
     # 在排除异常数据组之后，将GCYY数据流中的GCYY排序，计算所有GCYY的几何均值GM，
     df_all = pd.concat([df_yang, df_yin], axis=1, join='inner')
     # print(len(df_all.columns)) 
     GCYY = scipy.stats.gmean(df_all.iloc[:,1:len(df_all.columns)],axis=1)
     # print(GCYY)
     GM = scipy.stats.gmean(GCYY)
     # print(GM)
     # 计算每个GCYY与GM的定量差异，计算相继两个GCYY的定量差异，
     # 标出其中定量差异大于0.805的数据组，其中GCYY较低的标绿色，GCYY较高的标橙色
    
     arr_GCYY = np.array(GCYY)

     indexrGreen = np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805) & (arr_GCYY < GM))
     indexrOrange = np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805) & (arr_GCYY > GM))
     # print(np.where((abs(np.log(arr_GCYY/GM)/np.log(0.618)) > 0.805)))
     # print(len(indexrGreen[0]))
     # print(len(indexrOrange[0]))


     # In[ ]:

     # write to csv
     # print(df)
     # import os  
     # os.makedirs('subfolder', exist_ok=True)  
     df_all['GCYY'] = GCYY
     # df_all['time'] = pd.to_datetime(df['时间'], format='%Y-%m-%d %H:%M:%S')
     df_all['time'] = pd.to_datetime(df['时间'], format='%Y-%m-%d %H:%M:%S')
     df_all = df_all.drop(df_all[(df_all['GCYY'] == 1)].index)
     # df_csv = pd.concat([df_all, df_yin], axis=1, join='inner')
     output = STATICFILES_DIRS + table_name + '_report' + '.csv'
     # print("分析报告已保存至:",output)
     df_csv = df_all.copy()
     # print(df_csv)
     for (index, colname) in enumerate(df_csv):
          # print('df_csv: ', index, colname)
          if ((colname == '心率') | (colname == '低压') | (colname == '高压')):
               df_csv[colname] = 1/df_csv[colname]
     # print(df_csv)

     # 调整2， 计算每一分钟gcyy的平均值，只评估安静状态的gcyy
     df_csv['time'] = df_csv['time'].dt.floor('T')
     # print(df_csv)
     grouped_df = df_csv.groupby(['time'])
     mean_df = grouped_df.mean()
     mean_df = mean_df.reset_index()

     # print(mean_df)

     mean_df.to_csv(output, index=False)
     print("正在保存CSV文件")
     print("文件已经保存✅")

     print("------------------------------------------------")
     print("报告分析已完成🎉🎉🎉")

     now_time = dt.datetime.now().strftime('%F %T')
     print("报告出具时间:",now_time)

     # 将文件上传至IPFS

     construct_file_name_and_upload_csv_file(IMEI_number)

     # 将整理后的数据直接保存到数据库
     
     # create_engine("数据库类型+数据库驱动://数据库用户名:数据库密码@IP地址:端口/数据库"，其他参数)
     # engine=create_engine("mysql+pymysql://root:2022jianquanqin,@localhost:3306/water_data_analyzed",echo=True)
     # df_csv.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
     # print("store data successfully")


# 获取文件目录
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATICFILES_DIRS = os.path.join(HERE, 'static/csv/')

GATEWAY = "https://api-ap-singapore-1.getway.sinso.io"
URL = "/v1/upload"
CONTENT_TYPE = "text/csv"
FILE_PATH = STATICFILES_DIRS

FILENAME = "*"
TOKEN = "34b6b494bb0111ec8a58020017009841"


def upload(url, token, content_type, file):
    headers = {
        "Content-Type": content_type,
        "Token": token
    }
    params = {
        "name": FILENAME
    }
    response = requests.post(url=url, data=file, headers=headers, params=params)
    print(response.text)

def construct_file_name_and_upload_csv_file(IMEI_number):
     
     FILENAME = 'water_data_' + IMEI_number + '_report' + '.csv'

     print("文件名称是:",FILENAME)

     print("文件查找路径是:",FILE_PATH)

     f = open(os.path.join(FILE_PATH, FILENAME), "rb")
     upload(GATEWAY+URL, TOKEN, CONTENT_TYPE, f)

     print("文件已经上传至IPFS layer2 网络 Sinso")