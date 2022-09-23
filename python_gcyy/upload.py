import requests
import os

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


if __name__ == '__main__':

    f = open(os.path.join(FILE_PATH, FILENAME), "rb")
    upload(GATEWAY+URL, TOKEN, CONTENT_TYPE, f)


def construct_file_name_and_upload_csv_file(IMEI_number):

     FILENAME = 'water_data_' + IMEI_number + '_report' + '.csv'

     f = open(os.path.join(FILE_PATH, FILENAME), "rb")
     upload(GATEWAY+URL, TOKEN, CONTENT_TYPE, f)

     print("文件已经上传至IPFS layer2 网络 Sinso")


     