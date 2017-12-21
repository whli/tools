#encoding = utf-8

import os
import time
import datetime
import json
import sys 
reload(sys)
sys.setdefaultencoding('utf8')


class GetDateList(object):

    def __init__(self):
        pass

    def from_to_date(self, begin_day, end_day):  
        data_day = begin_day      
        tar_day = datetime.datetime.strptime(data_day,'%Y%m%d')
        num_day = 0
        date_list = []
        while data_day <= end_day:
            date_list.append(data_day)
            num_day += 1
            data_day = tar_day + datetime.timedelta(days = num_day)
            data_day = data_day.strftime("%Y%m%d")
        return date_list

    def from_to_num(self, begin_num, end_num):
        date_list = []
        for i in range(begin_num, end_num+1):
            data_day = datetime.date.today() - datetime.timedelta(days = i)
            data_day = data_day.strftime("%Y%m%d")
            date_list.append(data_day)
        return date_list

    def from_day_num(self, begin_day, num):
        tar_day = datetime.datetime.strptime(begin_day,'%Y%m%d')
        date_list = []
        for i in range(num):
            data_day = tar_day + datetime.timedelta(days = i)
            data_day = data_day.strftime("%Y%m%d")
            date_list.append(data_day)
        return date_list


if __name__ =="__main__":

    test_obj = GetDateList()
    date_list = test_obj.from_to_date("20170501", "20170520")
    num_list = test_obj.from_to_num(1, 7)
    date_num = test_obj.from_day_num("20170501", 7)

    for item in date_list:
        print item
    print "******"
    for item in num_list:
        print item
    print "******"
    for item in date_num:
        print item
