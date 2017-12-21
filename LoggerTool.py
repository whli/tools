# encoding=utf-8

import logging
import datetime

def get_logger(filename, log_level):
    # 文件后缀
    file_tail = datetime.datetime.now().strftime("_%Y%m%d")

    # 创建一个logger
    logger = logging.getLogger()

    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler("../log/%s.log" % (filename + file_tail))

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

def test():
    file_tail = datetime.datetime.now().strftime("_%Y%m%d")
    logging.basicConfig(level=logging.INFO, 
                        filename="../log/%s.log" % (__file__[:-3] + file_tail),
                        filemode='a', 
                        format='time=[%(asctime)s] level=[%(levelname)s] func_name=[%(filename)s:%(lineno)d:%(funcName)s] %(message)s')
    try:
        print a
    except Exception,e:
        s = sys.exc_info()
        logging.error("%s in line %d" %(s[1],s[2].tb_lineno))

if __name__ == "__main__":
    logger = get_logger("a")
    logger.info("test_log")
