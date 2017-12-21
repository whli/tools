#coding=utf-8

import datetime
import os

# 获取日期列表
def from_to_num(begin_num, end_num):
    date_list = []
    for i in range(begin_num, end_num+1):
        data_day = datetime.date.today() - datetime.timedelta(days = i)
        data_day = data_day.strftime("%Y%m%d")
        date_list.append(data_day)
    return date_list

# 清除特定目录下，带有某种标识的文件
def clear_log_file(path,topdown=True):
    date_list = from_to_num(0,6)
    flg_list = [date_str + ".log" for date_str in date_list]

    for root,dirs,files in os.walk(path,topdown):
        for tar_file in files:
            ary = tar_file.strip().split("_")
            flg = ary[-1]
            if flg not in flg_list:
                os.remove(os.path.join(root, tar_file))
            else:
                pass

# 清除特定目录下的pyc文件
def clear_pyc_file(path,topdown=True):
    for root,dirs,files in os.walk(path,topdown):
        for tar_file in files:
            if tar_file.find("pyc")>=0:
                os.remove(os.path.join(root, tar_file))

# 展示特定目录下的文件
def show_file(path,topdown=True):
    for root,dirs,files in os.walk(path,topdown):
        for tar_file in files:
            print tar_file

if __name__ == "__main__":
    path = "/home/hadoop/weihua/MXDataAnalyze/BetaStatisShow/"
    #clear_log_file(path)
    clear_pyc_file(path)
    show_file(path)
