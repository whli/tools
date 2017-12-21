# encoding=utf-8


from __future__ import print_function # Python 2/3 compatibility
import sys
import os
sys.path.append('../conf/')
sys.path.append('../util/')
reload(sys)
sys.setdefaultencoding('utf8')

import boto3
import json
import logging
import decimal
import datetime
import time

from config import *
import LoggerTool
from ElasticSearchUtil import ElasticSearchUtil
import SendS3Tool as s3_tool



class WriteToEs(object):

	def __init__(self):
		self.se_util = ElasticSearchUtil()

	# 将s3上的数据聚合到本地
	def get_s3_file(self, s3_path, file_name):

	    # 设置临时文件路径
	    locale_file = file_name
	    cleanfile = "rm %s" %(locale_file)
	    os.system(cleanfile)  # 清除文件，以便下次运行
	    locale_path = "%s_path" %locale_file

	    # s3数据获取
	    #s3_file = "%s%s" %(s3_base, date)
	    loadcmd = "/usr/bin/aws s3 sync %s %s --region ap-northeast-2" %(s3_path, locale_path)
	    os.system(loadcmd)
	    mergecmd = "cat %s/* > %s" %(locale_path, locale_file)
	    os.system(mergecmd)
	    cleancmd = "rm -r %s" %(locale_path)
	    os.system(cleancmd)

	# 获取最新需要更新数据的docid和topic list 
	def get_doc_topic(self, history_doc_path, new_data_path):
		mkdir_util("../data")
		history_id = "../data/history_doc_id"
		self.get_s3_file(history_doc_path, history_id)
		new_data = "../data/new_data"
		self.get_s3_file(new_data_path, new_data)

		filter_set = set()  # 存储历史docid
		with open(history_id) as f:
			for line in f:
				key = line.strip()
				filter_set.add(key)

		new_data_dict = {}  # 待处理的新数据
		with open(new_data) as f:
			for line in f:
				ary = line.strip().split("\t")
				key = ary[0]
				if key in filter_set:
					pass
				else:
					topic_list = json.loads(ary[1])["topic_list"]
					new_data_dict[key] = topic_list
		return filter_set,new_data_dict

	# 在ES更新相关数据
	def update_es(self, index, doc_dict):
		try:
			for doc in doc_dict:
				res = self.se_util.find_same_doc(index,doc)
				if "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]) or "_source" not in res["hits"]["hits"][0]:
					pass
				else:
					doc_type = res["hits"]["hits"][0]["_type"]
					doc_source = {}
					topic_list = doc_dict[doc]
					doc_source["topic_list"] = topic_list
					self.se_util.update_one_doc(index, doc_type, doc, doc_source)
					logger.warning(doc)
		except Exception, e :
			logger.error("catch exception -> " + str(e))

	# 存储历史docid数据到s3
	def save_to_s3(self, filter_set, doc_dict, s3_bucket, s3_file, history_doc_path):
		mkdir_util("../data")
		result_file = "../data/docid_result"
		cleanfile = "rm %s" %result_file
		os.system(cleanfile)

		# 写数据到文件
		with open(result_file,"w") as f:
			for item in filter_set:
				item = item + "\n"
				f.write(item)
			for item in doc_dict:
                                item = item + "\n"
                                f.write(item)

		# 上传数据到s3
		mid_path = "s3://mx-hadoop/garbage_mid"
		os.system("/usr/bin/aws s3 mv %s %s --recursive --region ap-northeast-2" %(history_doc_path, mid_path))
		s3_obj = s3_tool.S3Tool(result_file, s3_file, s3_bucket)
		s3_obj.upload_flie()


def mkdir_util(path):
        """
        os.mkdir()只能用于创建单层目录，os.makedirs()可用户创建多层目录
        """
	flg = os.path.exists(path)
	if not flg:  #  判断../data路径是否存在，并在不存该路径的情况下创建路径
		os.mkdir(path)
	else:
		pass

def test_get():
	data = {
		"92f723278730afc0c45faa75612f9fc1":[3,2,1,6],
		"e50e3f54f98a574615523661cd129974":[3,2,1,6]
		}
	return data

def run_main(history_doc_path, new_data_path, s3_bucket, s3_file):
	index = BUILD_COUNTRIES['KR']
	wr_obj = WriteToEs()

	# 产出基础数据
	#filter_set,new_data_dict = wr_obj.get_doc_topic(history_doc_path, new_data_path)
	new_data_dict = test_get()  # 用于进行更新操作的测试
	#filter_set = set()

	# 进行更新或保存操作
	wr_obj.update_es(index, new_data_dict)
	#wr_obj.save_to_s3(filter_set, new_data_dict, s3_bucket, s3_file, history_doc_path)

	# for test
	#for key in new_data_dict:
	#	print(key,new_data_dict[key])


if __name__ == "__main__":
	
	mkdir_util("../log")
	pyname = sys.argv[0][:-3]
	logger = LoggerTool.get_logger(pyname)

	history_doc_path = sys.argv[1]
	new_data_path = sys.argv[2]
	s3_bucket = sys.argv[3]
	s3_file = sys.argv[4]
	run_main(history_doc_path, new_data_path, s3_bucket, s3_file)


