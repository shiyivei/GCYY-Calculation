from django.http import HttpResponse
from python_gcyy.get_data_and_analyze import connect_and_fetch_data as get_report
from django.http import FileResponse

import json
import pymysql
import pandas as pd
from sqlalchemy import create_engine

def home_handler(request):
     print("welcome to home page")

     # start_response("200 OK ",[('Content-Type','text/html;charset=utf-8')])

     # return [bytes('<h2>This is Home Page</h2>',encoding='utf-8')]

     html = "<h1>This is Home Page</h1>"

     return HttpResponse(html)

def index_handler(request):

     print("路径访问成功")

     print("request.path_info:",request.path_info)
     print("request.method:",request.method)
     print("request.get:",request.GET)

     if request.method == "GET":

          IMEI_number=request.GET.get('IMEI_number',default='0')
          start_time=request.GET.get('start_time',default='0')
          end_time = request.GET.get('end_time',default='0')

          if IMEI_number == '0':
               from django.shortcuts import render
               return render(request, 'unbind.html')

  
          print("获取到的GET参数:",IMEI_number,start_time,end_time)   

          # 如果数据处理遇到问题，返回无效提示页面
          if get_report(IMEI_number,start_time,end_time):
               
               from django.shortcuts import render
               return render(request, 'invalid.html')

          else:

               context = {}
               personal_report = 'water_data_' + IMEI_number + '_report' + '.csv'

               context['personal_report'] = personal_report

               # print("正在渲染数据:",personal_report)
  
               # print("正在发送IMEI_number给d3发送GET请求")
               # context['IMEI_number'] = IMEI_number
               # print("正在传递报告名称给前端,前端读取渲染")
   
               from django.shortcuts import render
               # print("恭喜，数据已渲染完毕🎉🎉🎉")

               print()
               return render(request, 'index.html',context)

     else:

          html = "<h1>这不是一个 GET 请求</h1>"
          return HttpResponse(html) 


# 将数据保存至数据库，然后前端通过API访问读取

def txt_handler(request):
     
     # url编码模块v
     from urllib import parse

     filename = parse.quote('t018ESSzw1.txt')
     response = FileResponse("964242cbae57e444cdbb920514ba7ebc")

     response['Content-Type'] = 'text/plain'
     return response



def data_source(request):
    print("Welcome to index page")

    if request.method == "GET":
          
          IMEI_number=request.GET.get('IMEI_number',default='0')

          if IMEI_number == '0':
               from django.shortcuts import render
               return render(request, 'unbind.html')

          print("正在从数据库中获取数据并将其转换为json对象")    

          data_frame = get_analyzed_data(IMEI_number)
          print("获取到的数据对象:",data_frame)

          print("将数据对象转换为json对象")
          json_data = analyzed_data_to_json(data_frame)

          # print("转换好的json对象:",json_data)
          print("json对象已经成功转换")

          return HttpResponse(json.dumps(json_data)) # 返回json数据


def get_analyzed_data(IMEI_number):


     print("正在从数据库中获取报告数据")

     personal_report_table = 'water_data_' + IMEI_number

     engine=create_engine("mysql+pymysql://root:2022jianquanqin,@localhost:3306/water_data_analyzed",echo=True)

     result = pd.io.sql.read_sql_table(personal_report_table, engine)

     return result


def analyzed_data_to_json(dataFrame,orient='split'):
     df_json = dataFrame.to_json(orient = orient, force_ascii = False)
     return json.loads(df_json)