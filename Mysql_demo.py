#coding=utf-8

import os
import sys 
import datetime
import time
import  numpy as  np
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append("../conf")
import json
import MySQLdb
from pyspark import SparkContext,SparkConf
from config_show import *

def get_feature(card_config):
    conf_file = open(card_config)
    line = json.loads(conf_file.read())
    card_to_tab = {}
    for bran in line:
        for item in bran["logics"]:
            level = item.get("level","")
            card = item.get("card", "")
            if card == "" or level == "" or level != 'Album':
                continue
            for data_id in item.get("dataIdList"):
                card_to_tab[data_id] = card

    conf_file.close()
    return card_to_tab

def process_test(line):
    item_log = json.loads(line)
    event = item_log.get("event","")
    versionName = item_log.get("versionName","1.3.2")
    fromStack = item_log.get("fromStack","")
    if fromStack != "":
        print type(fromStack)
        for item in json.loads(fromStack):
            print item,type(item)

def process(line):
    try:
        item_log = json.loads(line)
        logId = item_log.get("logId","")
        uuid = item_log.get("uuid","")
        event = item_log.get("event","")
        versionName = item_log.get("versionName","1.3.2")
        fromStack = item_log.get("fromStack","")
        #event_set = set(["onlinePlayExit","albumClicked","seasonClicked"])
        album_set = set(["albumClicked","seasonClicked"])
        card_flg = ""
        album_flg = ""
        tab_flg = ""
        value_list = [0]*result_data_num  # pulltime,duration,card show ,card_click,album_show, album_click
        if versionName >= "1.3.5" and fromStack != "":
            fromStack = json.loads(fromStack)
            for item in fromStack: # 依次解析，遇到tab终止
                item_type = item.get("type","")
                item_id = item.get("id","")
                if item_type in ["albumDetail","seasonVideoList"]:
                    album_flg = item_id
                elif item_type == "card":
                    card_flg = item_id
                elif item_type == "tab":
                    tab_flg = item_id
                    break
                else:
                    pass
            
            if event in album_set:  # 针对album_click
                value_list[3] = 1
            if event == "onlinePlayExit":   # 针对video play
                if tab_flg == "Buzz" and card_flg == "":    # Buzz下的video list
                    card_flg = buzz_nocard
                if album_flg == "": # card click
                    value_list[3] = 1
                else:   # video play
                    value_list[5] = 1
                duration = item_log.get("duration","0") # play duration
                try:
                    duration = int(duration)
                except:
                    duration = 0
                if duration > 0:    # 存在为负数的情况
                    value_list[1] = duration
                else:
                    value_list[1] = 0
        if card_flg == "buzzCardChannelList":
            card_flg = item_log.get("mxVideoCategoryId","") # 对BUZZ下的card进行特殊处理
        if card_flg != "" and sum(value_list) > 0:
            key = logId + "_" + uuid
            return (key,(card_flg,value_list))
            #return 1    # for test
        return -1
    except:
        return -1
        #return 2    # for test

def process_server(line,feature_map):
    item = json.loads(line)
    log_type = item.get("logtype","")
    container_type = item.get("container_type","")
    context = item.get("context","")
    container_id = item.get("container_id","")
    type_dict = item.get("attachment") # 判断是album 还是channel  video 暂时不考虑
    appVersion = item.get("appVersion",1170000132)
    if appVersion < 1170000135: # 只处理1170000135之后的版本
        return -1
    card = ""
    video_id = None
    value_list = [0]*result_data_num  # pulltime,duration,card show ,card_click,album_show, album_click
    if log_type == "v3_resource_show":
        if context in ["tab","tablist",None]:
            if container_type =="tab":  #BUZZ
                card = buzz_nocard  #buzz nocard
                value_list[0] = 1
                value_list[2] =item.get("feeds_len")
            elif container_type in card_type_set: # card
                card = type_dict['channel_id']
                value_list[0] = 1
                value_list[2] =item.get("feeds_len")
            elif container_type in album_type_set: #album
                if "video_id" in type_dict:
                    video_id = type_dict.get("video_id",None)
                if 'season_id' in type_dict:
                    card = feature_map.get(type_dict['season_id'],"")
                elif 'album_id' in type_dict:
                    card = feature_map.get(type_dict['album_id'],"")
                else:
                    pass
                if video_id is None:
                    value_list[4] =item.get("feeds_len")
            if card != "" and sum(value_list) > 0:
                return (card,value_list)
    return -1

def merge(x,y):
    for index,value in enumerate(x):
        y[index] += value
    return y

def get_client_log(sc,year,month,day):
    in_path = beta_tracking_log + "/%s/%s/%s/*/*" %(year,month,day)
    lines = sc.textFile(in_path)
    lines = lines.map(lambda line:process(line)).filter(lambda line:line != -1).reduceByKey(lambda x,y:x)   # 去除重复日志
    lines = lines.map(lambda item:item[1]).reduceByKey(lambda x,y:merge(x,y))
    return lines
    for line in lines.take(100):
        print line 

def test(sc):
    in_path = beta_tracking_log + "/2017/12/12/*/*"
    lines = sc.textFile(in_path).map(lambda line:process(line)).filter(lambda line:line != -1).countByValue()
    print lines

def get_server_log(sc,year,month,day):
    in_path = server_log + "/%s/%s/%s/*/*" %(year,month,day)
    lines = sc.textFile(in_path)
    feature_map = get_feature(card_config_new)
    feature_bc =sc.broadcast(feature_map)
    lines = lines.map(lambda line:process_server(line,feature_bc.value)).filter(lambda line:line != -1).reduceByKey(lambda x,y:merge(x,y))
    return lines
    for line in lines.take(1000):
        print line

def process_result(item):
    card_id = item[0]
    pull_times = item[1][0]
    card_show = item[1][2]
    card_click = item[1][3]
    album_show = item[1][4]
    album_click = item[1][5]
    if pull_times == 0:
        return (card_id,[0]*result_data_num)
    if card_show < card_click:
        card_click = card_show
    if album_show < album_click:
        album_click = album_show
    return (card_id,[pull_times,item[1][1],card_show,card_click,album_show,album_click])
    
def from_to_num(begin_num, end_num):
    date_list = []
    for i in range(begin_num, end_num+1):
        data_day = datetime.date.today() - datetime.timedelta(days = i)
        data_day = data_day.strftime("%Y%m%d")
        date_list.append(data_day)
    return date_list

def write_to_db(value_list,cday):
    conn = MySQLdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pw, db=db_name, charset="utf8")
    cursor = conn.cursor()
    sql_table = '''
                create table if not exists mx_beta_card_new(
                        id int not null auto_increment,
                        card_id varchar(60),
                        data_date date,
                        ctime datetime,
                        pull_times int,
                        play_time int,
                        card_show int,
                        card_click int,
                        card_rate float,
                        album_show int,
                        album_click int,
                        album_rate float,
                        constraint card_pk primary key(id));
    '''
    #cursor.execute(sql_table)
    #conn.commit()

    sql_insert = '''
                insert into mx_beta_card_new(
                        card_id,data_date,ctime,pull_times,play_time,
                        card_show,card_click,card_rate,
                        album_show,album_click,album_rate
                )
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    '''
    for item in value_list:
        card_id = item[0]
        pull_times,play_time,card_show,card_click,album_show,album_click = item[1]
        card_rate = float(card_click)/card_show if card_show > 0 else 0
        album_rate = float(album_click)/album_show if album_show > 0 else 0    
        ctime = time.strftime('%Y-%m-%d %X',time.localtime(time.time()))
        #cday = time.strftime('%Y-%m-%d',time.localtime(time.time())) 
        param = (card_id,cday,ctime,pull_times,play_time,
                    card_show,card_click,card_rate,
                    album_show,album_click,album_rate)
        cursor.execute(sql_insert,param)
    conn.commit()
    conn.close()

def db_test(cday):
    conn = MySQLdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pw, db=db_name, charset="utf8")
    cursor = conn.cursor()
    sql_select = '''
            select * from %s
    ''' %("mx_beta_card_new")
    cursor.execute(sql_select)
    result = list(cursor.fetchall())
    for item in result:
        print item
    conn.close()

def run(sc):
    date_list = from_to_num(1,1)
    for ymd in date_list:
        year = ymd[:4]
        month = ymd[4:6]
        day = ymd[6:8]
        client_rdd = get_client_log(sc,year,month,day)
        server_rdd = get_server_log(sc,year,month,day)
        union_rdd = client_rdd.union(server_rdd)
        result_rdd = union_rdd.reduceByKey(lambda x,y:merge(x,y))#.filter(lambda item:False if item[1][0] == 0 and item[1][1] > 0 else True)
        result_rdd = result_rdd.map(lambda item:process_result(item)).filter(lambda item:sum(item[1])>0)   # 由于是模糊统计，会存在一些异常数据，需对其进行简单处理
        value_list = result_rdd.collect()
        ymd_date = datetime.datetime.strptime(ymd, "%Y%m%d")
        data_date = ymd_date.strftime("%Y-%m-%d")
        write_to_db(value_list,data_date)    # write
        
        print "card_id","pull_times","play_time","card_show","card_click","card_rate","album_show","album_click","album_rate"
        for item in value_list:
            card_id = item[0]
            pull_times,play_time,card_show,card_click,album_show,album_click = item[1]
            card_rate = float(card_click)/card_show if card_show > 0 else 0
            album_rate = float(album_click)/album_show if album_show > 0 else 0
            print card_id,pull_times,play_time,card_show,card_click,card_rate,album_show,album_click,album_rate
        print "******"

if __name__ == "__main__":
    sc = SparkContext(appName="Card_Statis_New")
    sc.setLogLevel("WARN")
    #get_server_log(sc,"2017","12","12")
    #get_client_log(sc,"2017","12","12")
    run(sc)
    #db_test("2017-12-12")
