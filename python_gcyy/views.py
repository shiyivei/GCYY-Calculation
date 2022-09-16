from django.http import HttpResponse
from python_gcyy.get_data_and_analyze import connect_and_fetch_data as get_report
from django.http import FileResponse

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

          # IMEI_number=request.GET.get('IMEI_number',default='F929435DD002')
          # start_time=request.GET.get('start_time',default='1662448705')
          # end_time = request.GET.get('end_time',default='1662535105')

          IMEI_number=request.GET.get('IMEI_number',default='0')
          start_time=request.GET.get('start_time',default='0')
          end_time = request.GET.get('end_time',default='0')



          if IMEI_number == '0':
               from django.shortcuts import render
               return render(request, 'unbind.html')

          # 时间判断逻辑
          # if start_time >= end_time:

          #      html = "<div style='position: relative;text-align: center;font-size:24px;top:422px;height:500px;background-color: rgb(255, 255, 255);'><h1>报告开始时间不能早于结束时间</h1><div>"
          #      return HttpResponse(html) 
  
          print("Got parameters are:",IMEI_number,start_time,end_time)   

          if get_report(IMEI_number,start_time,end_time):
               
               from django.shortcuts import render
               return render(request, 'invalid.html')

          else:

               personal_report = 'water_data_' + IMEI_number + '_report' + '.csv'

               print("正在渲染数据:",personal_report)

               context = {}
               context['personal_report'] = personal_report

               from django.shortcuts import render
               print("恭喜，数据已渲染完毕🎉🎉🎉")
               print()
               return render(request, 'index.html',context)

     else:

          html = "<h1>这不是一个 GET 请求</h1>"
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