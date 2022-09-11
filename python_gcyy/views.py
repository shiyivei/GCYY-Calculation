from pydoc import plain
import re
from django.http import HttpResponse
from python_gcyy.get_data_and_analyze import connect_and_fetch_data as get_report
from django.http import FileResponse
import os

def home_handler(request):
     print("welcome to home page")

     # start_response("200 OK ",[('Content-Type','text/html;charset=utf-8')])

     # return [bytes('<h2>This is Home Page</h2>',encoding='utf-8')]

     html = "<h1>This is Home Page</h1>"

     return HttpResponse(html)

def index_handler(request):
     print("Welcome to index page")

     # start_response("200 OK ",[('Content-Type','text/html;charset=utf-8')])

     # return [bytes('<h2>This is Home Page</h2>',encoding='utf-8')]


     print("request.path_info:",request.path_info)
     print("request.method:",request.method)
     print("request.get:",request.GET)

     if request.method == "GET":

          IMEI_number=request.GET.get('IMEI_number',default='F929435DD002')
          start_time=request.GET.get('start_time',default='1662448705')
          end_time = request.GET.get('end_time',default='1662535105')

          print("Got parameters are:",IMEI_number,start_time,end_time)   

          get_report(IMEI_number,start_time,end_time)
     

          from django.shortcuts import render

          return render(request, 'index.html')

     else:

          html = "<h1>It's not a GET Request</h1>"

          return HttpResponse(html) 



def txt_handler(request):
     
     # url编码模块
     from urllib import parse

     filename = parse.quote('t018ESSzw1.txt')
     response = FileResponse("964242cbae57e444cdbb920514ba7ebc")

     # file_path = os.path.abspath('t018ESSzw1.txt')
     # response = FileResponse(filename, file_path)

     # 各种文件后缀对应的Content-Type可以去下面博客中查找
     # https://blog.csdn.net/judge9999/article/details/1496945
     response['Content-Type'] = 'text/plain'
     # response['content-disposition'] = 'attachment;filename'
     return response