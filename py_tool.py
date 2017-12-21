#coding=utf-8

import os
import datetime
import logging

def mkdir_util(path):
    """ 
    os.mkdir()只能用于创建单层目录，os.makedirs()可用户创建多层目录
    """
    flg = os.path.exists(path)
    if not flg:  #  判断../data路径是否存在，并在不存该路径的情况下创建路径
            os.makedirs(path)
    else:
            pass

def file_exit_detect(file_path):
    """
    判断文件或目录是否存在
    """
    flg = os.path.exists(file_path)
    return flg

def get_logger(filename, log_level):
    """
    配置日志记录格式及日志文件目录
    """
    # 文件后缀
    file_tail = datetime.datetime.now().strftime("_%Y%m%d")

    # 创建一个logger
    logger = logging.getLogger()

    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler("%s.log" % (filename + file_tail))

    # 定义handler的输出格式formatter 
    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    fh.setFormatter(formatter)

    # 给logger添加handler
    logger.addHandler(fh)

    # 设置日志级别
    # 日志级别等级CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET 
    #logger.setLevel(logging.WARNING)
    logger.setLevel(log_level)    

    return logger

if __name__ == "__main__":
    print "基础工具封装"
