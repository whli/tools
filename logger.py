#-*-coding:utf8-*-
"""
"""
import logging
from logging.handlers import RotatingFileHandler
import os


def produce_logger(logger_name, log_file, DebugOrNot = False):

	abs_path = os.path.dirname(os.path.abspath(__file__))
	abs_father_path = os.path.dirname(abs_path)
	log_dir_path = abs_father_path + '/log'
	if not os.path.exists(log_dir_path):
		os.makedirs(log_dir_path)
	func_logger = logging.getLogger(logger_name)
	formatter = logging.Formatter('[%(asctime)s][pid:%(process)s-tid:%(thread)s] %(module)s.%(funcName)s: %(levelname)s: %(message)s')
	
	fhr_tmp = RotatingFileHandler('%s/%s'%(log_dir_path, log_file), maxBytes=10*1024*1024, backupCount=3)
	fhr_tmp.setFormatter(formatter)
	fhr_tmp.setLevel(logging.DEBUG)
	
	hdr_screen = logging.StreamHandler()
	hdr_screen.setFormatter(formatter)
	hdr_screen.setLevel(logging.DEBUG)
	
	func_logger.addHandler(fhr_tmp)
	if DebugOrNot:
		func_logger.addHandler(hdr_screen)
    	func_logger.setLevel(logging.DEBUG) #lowest debug level for logger
	if not DebugOrNot:
		func_logger.setLevel(logging.ERROR) #lowest debug level for logger
		
	return func_logger
	

if __name__ == '__main__':
    '''
    Usage:

    logger_tmp = produce_logger("model","no_buy.log")
    logger_tmp.error("lkkkkk")
    '''
    logger_tmp = produce_logger("model","no_buy.log")
    logger_tmp.error("lkkkkk")
