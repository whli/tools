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
conf = SparkConf().setAppName("Knowledge_info_click")
sc = SparkContext(conf=conf)


## def fun to process data for every line
def process_line(line):
    
    ## get content of a log
    str1=re.match(r'.*content\[(.*)\]',line)
    if str1 is not None:
       jy_str=str1.group(1)
    else:
       jy_str=""
    
    ## get info of 
    know_str=""
    flg = 0
    try:
      if jy_str !="":
         #替换特殊字符，防止输出时出错
	 jy_str = jy_str.replace('\\r','')
	 jy_str = jy_str.replace('\\t','')
	 #json字符串转换
         jy_dict=json.loads(jy_str)
         if jy_dict.has_key("type") and jy_dict.has_key("messagetype"):
            
	    if jy_dict["type"] == "adminMessage" and jy_dict["messagetype"] == "feedback":
               flg = 1
	       know_str=str(jy_dict["fromUserId"])+ "||" + jy_dict["type"].encode("utf-8","ignore")

	    elif jy_dict["type"] == "studentfeedback" and jy_dict["messagetype"] == "feedback":
	       flg = 1
	       know_str=str(jy_dict["userid"])+ "||" + jy_dict["type"].encode("utf-8","ignore") + "||" + jy_dict["msg"].encode("utf-8","ignore")
            
	    else:
               return ('Error',0)
         else:
            return ('Error',0)
      else:
         return ('Error',0)
    except Exception:
      return ('Error',0)
    

    ## get rid of a log
    if flg == 1:
      str2=re.match(r'.*rid\[(.*?)\] ',line)
      if str2 is not None:
         rid_str=str2.group(1)
      else:
         rid_str=""

      if rid_str !="":
         know_str=know_str+"||"+str(rid_str)
         return (know_str,1)
      else:
         return ('Error',0)

    else:
      return ('Error',0)

## 
#start_day=time.strftime('%Y-%m-%d',time.localtime(time.time()))
data_path = sys.argv[1]


## Get data from hadoop
lines = sc.textFile("%s" %(data_path))
lines = lines.map(lambda line:process_line(line)).filter(lambda line:'Error' not in line).reduceByKey(lambda a,b:a+b)


## Save Dstream data to hadoop
lines.saveAsTextFile("/haibian/liweihua/classroom/info_click")

