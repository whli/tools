#coding=utf-8


## Import module that we need
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import json
import re

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext


## Set the sparkstreaming
conf = SparkConf().setAppName("Course_data_chuqin")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,120)


## def fun to process data for every line
def process_line(line):
     #get content of a log
    str1=re.match(r'.*data\[(.*?)\]',line)
    if str1 is not None:
       jy_str=str1.group(1)
       # print jy_str
    else:
      jy_str=""
    
    # get uri info
    uri_str=re.match(r'.*uri\[(.*?)\]',line)
    if uri_str is not None:
       uri_str=uri_str.group(1)
    else:
       uri_str=""

    #get info of user_id, mes from content
    flg=0
    chuqin_str=""
    try:
      if jy_str !="":
         jy_str = jy_str.replace('\\r','')
         jy_dict=json.loads(jy_str)
         if uri_str == "/class/user/send_online":
            flg=1
            chuqin_str=str(jy_dict["user_id"])+ "||" + str(jy_dict["chapter_id"])+ "||" + str(jy_dict["start_time"])
         else:
            pass
      else:
         pass
    except Exception:
      pass
    
    if flg ==1:
      # get time of the log
      str3=re.match(r'\[(.*?)\.',line)
      if str3 is not None:
         time_str=str3.group(1)
      else:
         time_str=""

      if time_str !="":
         chuqin_str=chuqin_str+"||"+str(time_str)
         # chuqin_str = user_id, chapterid, start_time, getin_time 
         #print chuqin_str
         return chuqin_str


start_day=time.strftime('%Y-%m-%d',time.localtime(time.time())) 

## Get data from hadoop
lines = ssc.textFileStream("/haibian_log/cdata_log_online_spark/")
lines = lines.map(lambda line:process_line(line))
lines.pprint()


## Save Dstream data to hadoop
#lines.saveAsTextFiles("/haibian/liweihua/ketang_statis/chuqin/ch")


## Start the spark process
ssc.start()
ssc.awaitTermination()


