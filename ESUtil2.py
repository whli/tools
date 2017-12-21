# encoding=utf-8

import sys
import os

sys.path.append('../conf/')
sys.path.append('../util/')
sys.path.append('../data/')
reload(sys)

import urllib2
import json
import logging.config
import decimal

from config import *
from elasticsearch import Elasticsearch
'''
Created on 2017年4月7日

@author: zhongrenli
'''

# Helper class to convert a DynamoDB item to JSON.
class DecimalToStringEncoder(json.JSONEncoder):
    def default(self, o):
        # TODO : int/long/float 不生效!
        if isinstance(o, int):
            return str(o)
        if isinstance(o, long):
            return str(o)
        if isinstance(o, float):
            return str(o)
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalToStringEncoder, self).default(o)

class ElasticSearchUtil(object):

    #批量建索引
    def import_index_bulk(self, es_endpoint, data_path) :
        try :
            if False == os.path.exists(data_path) :
                return -1
            length = os.path.getsize(data_path)
            length_KB = length / 1024
            if 0 == length :
                return 0
            json_data = open(data_path, "r")
            url = es_endpoint + '/_bulk'
            request = urllib2.Request(url, data=json_data)
            request.add_header('Cache-Control', 'no-cache')
            request.add_header('Content-Type', 'application/x-ndjson')
            request.add_header('Content-Length', '%d' % length)
            result = urllib2.urlopen(request, timeout=1000).read().strip()
            logging.info("import index success, bulk size : [%s]KB", length_KB)
            print result
            return 0
            
        except Exception, e :
            print str(e)
            logging.error("import index failed, error_info:[%s]", str(e))
            return -3
    
    #获取doc详情
    def get_doc_info(self, es_endpoint, info) :
        try :
            _id = info["md5"]
            url = es_endpoint + '/_search'
            m_data = {
                    "query": {
                        "match": {
                            "_id": "%s" % _id
                    }
                }
             }
            n_data=json.dumps(m_data)
            
            request = urllib2.Request(url, n_data)
            request.add_header('Cache-Control', 'no-cache')
            request.add_header('Content-Type', 'application/x-ndjson')
            request.get_method = lambda:'GET'
            
            response = urllib2.urlopen(request, timeout=1000).read()
            
            result = json.loads(response)
            result_num = result["hits"]["total"]
            
            if 0 < result_num :
                info["culture"] = result["hits"]["hits"][0]["_source"]["culture"]
                info["displayType"] = result["hits"]["hits"][0]["_source"]["displayType"]
                
                for (key, value) in MODIFY_ES_MAP.items() :
                    if (key in result["hits"]["hits"][0]["_source"]) :
                        info[key] = result["hits"]["hits"][0]["_source"][key]
                    else :
                        info[key] = "0" 
            else :
                return -1
            
            return 0
        except Exception, e :
            logging.error("get doc info failed, error_info:[%s]", str(e))
            print str(e)
            return -1
              
    #删除索引文件
    def delete_index(self, es_endpoint, info) :
        try :
            _id = info["md5"]
            _ix = info["culture"]
            url = es_endpoint + "/" + _ix +"/_delete_by_query"
            m_data = {
                "query": {
                    "match": {
                        "_id": "%s" % _id
                    }
                }
            }
            n_data=json.dumps(m_data)
            
            request = urllib2.Request(url, n_data)
            request.add_header('Cache-Control', 'no-cache')
            request.add_header('Content-Type', 'application/x-ndjson')
            request.get_method = lambda:'POST'
            
            result = urllib2.urlopen(request, timeout=1000).read()
            return 0
        except Exception, e :
            logging.error("delete index fail, error_info:[%s]", str(e))
            print str(e)
            return -1
        
    #更新索引文件
    def update_index(self, es_endpoint, info) :
        try :
            _id = info["md5"]
            _ix = info["culture"]
            _ty = info["displayType"]
            url = es_endpoint + "/" + _ix + "/" + _ty + "/" + _id +"/_update"
            
            for (key, value) in MODIFY_ES_MAP.items() :
                if (value in info["update"]) :
                    info["update"][value] = str(int(info["update"][value]) + int(info[key]))
                    info["update"][key] = info["update"][value]
                    info["update"].pop(value)
            
            m_data = {
                "doc": info["update"]
            }
            n_data=json.dumps(m_data)
            
            request = urllib2.Request(url, n_data)
            request.add_header('Cache-Control', 'no-cache')
            request.add_header('Content-Type', 'application/x-ndjson')
            request.get_method = lambda:'POST'
            
            result = urllib2.urlopen(request, timeout=1000).read()
            return 0
        except Exception, e :
            logging.error("update index fail, error_info:[%s]", str(e))
            print str(e)
            return -1
        
    #读取原始数据，返回dict
    def load_doc_info(self, json) :
        doc = {}
        for (key, value) in DOC_SEGMENTS.items() :
            if key not in json :
                if 1 == value :
                    return {}
                continue
            doc[key] = json[key]
        return doc
        
    #构造索引所需文件的头和内容
    def assemble_index_body(self, record) :
        result_obj = {}
        se_head = {}
        se_head["index"] = {}
        
        record_id = "null-id"
        if "md5" in record :
            record_id = record["md5"]
            
        record_culture = "null-culture"
        if "culture" in record :
            record_culture = record["culture"]
        
        record_type = "null-type"
        if "displayType" in record :
            record_type = record["displayType"]
            
        se_head["index"]["_type"]  = record_type
        se_head["index"]["_id"]    = record_id
        se_head["index"]["_index"] = record_culture
        
        for key in record :
            # 由于直接写在DecimalToStringEncoder中不生效, 所以只能写在这里了
            if isinstance(record[key], int):
                record[key] = str(record[key])
            if isinstance(record[key], long):
                record[key] = str(record[key])
            if isinstance(record[key], float):
                record[key] = str(record[key])
#             if "s3Info" == key and isinstance(record[key], dict) :
#                 record[key] = json.dumps(record[key])
                
        result_obj["head"] = str(json.dumps(se_head, cls=DecimalToStringEncoder))
        result_obj["body"] = str(json.dumps(record, cls=DecimalToStringEncoder))
        
        logging.debug ("id[%s] type[%s] to index[%s] queue", 
            record_id, record_type, record_culture)
            
        return result_obj
    
    #获取指定索引文件的doc数量
    def get_doc_num(self, es_endpoint, index) :
        if 'all' != index :
            url = es_endpoint + '/' + index + '/_stats/docs'
        else :
            url = es_endpoint + '/_stats/docs'
        
        result = ''
        try :
            request = urllib2.Request(url)
            request.get_method = lambda:'GET'
            result = json.loads(urllib2.urlopen(request, timeout=1).read().strip())
        except Exception, e :
            logging.error("get doc num failed, error_info:[%s]", str(e))
            return -1
        
        result_core = ''
        if 'all' == index :
            if "_all" not in result :
                return -2
            result_core = result["_all"]
        else :
            if "indices" not in result :
                return -3
            if index not in result["indices"] :
                return -4
            result_core = result["indices"][index]
            
        if "total" not in result_core :
            return -5
        if "docs" not in result_core["total"] :
            return -6
        if "count" not in result_core["total"]["docs"] :
            return -7

        return int(result_core["total"]["docs"]["count"])
    
    
if __name__ == '__main__':
    elastic_search_util = ElasticSearchUtil()
    print elastic_search_util.get_doc_num(ES_ENDPOINT, "en-photo")  
    
    
    
    
    