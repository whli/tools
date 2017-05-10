#coding=utf-8

import logging

# 创建一个logger
logger = logging.getLogger()

# 创建一个handler，用于写入日志文件
fh = logging.FileHandler('./log_file')

# 定义handler的输出格式formatter 
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
fh.setFormatter(formatter)

# 给logger添加handler
logger.addHandler(fh)

# 设置日志级别
# 日志级别等级CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET 
logger.setLevel(logging.DEBUG)


