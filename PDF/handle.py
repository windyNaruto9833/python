# -*- coding: utf-8 -*-
import datetime
import re
import sys
import time
from datetime import timedelta
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
import requests
import pymysql
import redis
import os

#日期校验
def isVaildDate(date):
    try:
        if ":" in date:
            time.strptime(date, "%Y-%m-%d %H:%M:%S")
        else:
            time.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False

#根据给定的日期，获取前n天或后n天的日期，n为正数则是以后的n天，n为负数则是以前的n天
def get_day_of_day(str2date,n=0):
    if(n<0):
        n = abs(n)
        return (str2date-timedelta(days=n))
    else:
        return str2date+timedelta(days=n)

#开始日期与结束日期校验
def valid_date(startDate,endDate):
    #获取当前时间日期
    # nowTime_str = datetime.datetime.now().strftime('%Y-%m-%d')
    # print(nowTime_str)
    #mktime参数为struc_time,将日期转化为秒，
    e_time = time.mktime(time.strptime(endDate,"%Y-%m-%d"))
    print(e_time)
    try:
        s_time = time.mktime(time.strptime(startDate, '%Y-%m-%d'))
        #print(s_time)
        #日期转化为int比较
        diff = int(e_time)-int(s_time)
        #print(diff)
        if diff >= 0:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False

#读取pdf文件到字符串
def getContet(pdfPath,pages=None):
    if(pdfPath is None):
        return
    print("pdfUrl===="+pdfPath)

    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)
    output = StringIO()
    # 创建一个PDF资源管理器对象来存储共赏资源
    manager = PDFResourceManager()
    # 创建一个PDF设备对象
    converter = TextConverter(manager, output, laparams=LAParams())
    # 创建一个PDF解释器对象
    interpreter = PDFPageInterpreter(manager, converter)

    # 打开源pdf文件
    with open(pdfPath, 'rb') as infile:
        # 对pdf每一页进行分析
        try:
            for page in PDFPage.get_pages(infile, pagenums):
                interpreter.process_page(page)
        except Exception as e:
            print(e)
            print("PDF内容提取失败")
    converter.close()
    # 得到每一页的txt文档
    text = output.getvalue()
    output.close
    #print(text)
    pattern = r'\s+'
    #logging.info("txt转换成的字符串==="+re.sub(pattern, '',text))
    #print(re.sub(pattern, '',text))
    content=re.sub(pattern, '',text)
    return  content

#下载pdf到本地
def downpdf(urlPDF):
    res = requests.get(urlPDF)
    if(res.status_code==200):
        #获得文件id
        pdfId = re.findall("[^/]+(?!.*/)", urlPDF)
        res.raise_for_status()
        #pdf_path="C:\\Users\\shangshibang\\Desktop\\ho\\0806\\"+str(pdfId[0])
        pdf_path="/pdf/"+str(pdfId[0])#linux下文件路径

        with open(pdf_path, 'wb') as file:
            for chunk in res.iter_content(100000):
                file.write(chunk)
        return pdf_path
    else:
        return

#获取配置文件信息
import configparser
config = configparser.ConfigParser()
config.read('property.conf',encoding='UTF-8')
#从数据库中获取pdf的下载链接
ip=config['mysql']['ip']
user=config['mysql']['user']
password=config['mysql']['password']
dabase=config['mysql']['dabase']
# 打开数据库连接
db = pymysql.connect(ip, user, password,dabase, charset='utf8' )
# 使用cursor()方法获取操作游标
cursor = db.cursor()
redisHost=config['redis']['ip']
redisPort=config['redis']['port']

rdp = redis.ConnectionPool(host=redisHost, port=redisPort, password="")
#redis连接池
r = redis.StrictRedis(connection_pool=rdp)

if __name__ == "__main__":
     if((len(sys.argv)<=2) or (len(sys.argv)>3)):
         raise Exception("请按格式执行脚本，如：python pdf_handle_store.py 2017-08-01 2017-08-30")

     #日期字符串校验
     frist = isVaildDate(sys.argv[1])
     if(not frist):
         raise Exception("请输入有效的日期")

     second = isVaildDate(sys.argv[2])
     if (not second):
         raise Exception("请输入有效的日期")

     result = valid_date(sys.argv[1], sys.argv[2])
     if(not result):
         raise Exception("开始时间不能大于结束时间")

     startDate=sys.argv[1]
     endDate=sys.argv[2]

     #获得脚本需要停止的日期
     strStop = endDate.replace("-", "")
     str2dateStop = datetime.datetime.strptime(strStop, "%Y%m%d")  # 字符串转化为date形式
     stopDate = get_day_of_day(str2dateStop, 1).date().strftime("%Y-%m-%d")
     #根据当前日期获取明天的日期
     while True:
         print(startDate)
         #数据查询及文件下载，PDF内容抽取===================================开始
         sql = "select url,notice_id from xxxx where public_time like '%" + startDate + "%' and content is null and (url like '%pdf%' or url like '%PDF%')"
         db.ping(reconnect=True)
         cursor.execute(sql)
         results = cursor.fetchall()

         for row in results:
             url = row[0]
             notice_id = row[1]
             pdfPath = downpdf(url)
             print(pdfPath)
             print(notice_id)
             # content中的单引号替换为双引号，以免插入数据库时因为引号的问题导致语法错误无法插入
             pdfContent=getContet(pdfPath)
             if(pdfContent is not None):
                 content = pdfContent.replace('\'', '\"')
                 #将抽取内容后的PDF删除
                 if os.path.exists(pdfPath):  # 如果文件存在
                     # 删除文件
                     os.remove(pdfPath)
                 else:
                     print('no such file:%s' % pdfPath)  # 则返回文件不存在
                 sql = "update ssb_insight_notice set xxxx.content='%s',es_store=0 where notice_id=%s" % (
                 content, notice_id)
                 with r.pipeline(transaction=False) as p:
                     try:
                         cursor.execute(sql)
                         db.commit()
                         #将成功解析的notice_id插入redis success_url
                         p.sadd("success_noticeId", notice_id)
                     except Exception as e:
                         print(e)
                         print("修改失败")
                         p.sadd("fail_noticeId", notice_id)
                         #将成功解析的notice_id插入redis fail_noticeId
                     finally:
                        p.execute()
         db.close()
         #数据查询及文件下载，PDF内容抽取===================================结束
         strContent = startDate.replace("-", "")
         str2date = datetime.datetime.strptime(strContent, "%Y%m%d")  # 字符串转化为date形式
         nextDay = get_day_of_day(str2date, 1).date().strftime("%Y-%m-%d")
         startDate=nextDay
         if(startDate==stopDate):
             break
