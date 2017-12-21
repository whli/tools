#coding:utf-8
#ElasticSearch相关
import sys
import os
sys.path.append('../conf/')
reload(sys)
sys.setdefaultencoding('utf8')

import boto3
import json
import decimal
import urllib2
import datetime
import logging
from boto3.dynamodb.conditions import Key, Attr
from config import *
from elasticsearch import Elasticsearch, RequestsHttpConnection
from TextComparer import TextComparer

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
               return float(o)
            else:
               return int(o)
        return super(DecimalEncoder, self).default(o)

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

class ElasticSearchUtil:

    def __init__(self):
        self.es_client = Elasticsearch(
            hosts=[{'host': ES_ENDPOINT, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    def import_one_doc(self, es_index, es_type, es_id, es_body) :
        if "s3Info" in es_body :
            es_body.pop("s3Info")
        if "selfChannel" in es_body and 0 < len(es_body["selfChannel"]):
            #print("import failed, no selfChannel, index[%s] type[%s] id[%s] body[%s]" %(es_index, es_type, es_id, json.dumps(es_body)))
            #return
            channel = es_body["selfChannel"][0].strip()
            es_body["searchChannel"] = channel
            channels = channel.split(".")
            for i in range(0, len(channels)) :
                channel_key = "searchChannel_" + str(i)
                channel_value = channels[i]
                es_body[channel_key] = channel_value
        for key in es_body :
            # 由于直接写在DecimalToStringEncoder中不生效, 所以只能写在这里了
            if isinstance(es_body[key], int):
                es_body[key] = str(es_body[key])
            if isinstance(es_body[key], long):
                es_body[key] = str(es_body[key])
            if isinstance(es_body[key], float):
                es_body[key] = str(es_body[key])

        new_body = json.loads(json.dumps(es_body, cls=DecimalToStringEncoder))

        res = self.es_client.index(es_index, es_type, new_body, id=es_id)

        if "_shards" not in res or "successful" not in res["_shards"] or 0 >= int(res["_shards"]["successful"]) :
            print("import failed, index[%s] type[%s] id[%s] body[%s]" %(es_index, es_type, es_id, json.dumps(new_body)))
        else :
            print("import successful, index[%s] type[%s] id[%s]" %(es_index, es_type, es_id))
        return

    def update_one_doc(self, es_index, es_type, es_id, es_body) :
        if "s3Info" in es_body :
            es_body.pop("s3Info")
        for key in es_body :
            # 由于直接写在DecimalToStringEncoder中不生效, 所以只能写在这里了
            if isinstance(es_body[key], int):
                es_body[key] = str(es_body[key])
            if isinstance(es_body[key], long):
                es_body[key] = str(es_body[key])
            if isinstance(es_body[key], float):
                es_body[key] = str(es_body[key])
        new_body = json.loads(json.dumps(es_body, cls=DecimalToStringEncoder))

        update_body = {}
        update_body["doc"] = new_body
        res = self.es_client.update(es_index, es_type, es_id, update_body)
        if "_shards" not in res or "successful" not in res["_shards"] or 0 >= int(res["_shards"]["successful"]) :
            print("update failed, index[%s] type[%s] id[%s] update_body[%s]" %(es_index, es_type, es_id, str(json.dumps(es_body))))
            return False
        else :
            print("update successful, index[%s] type[%s] id[%s] update_body[%s]" %(es_index, es_type, es_id, str(json.dumps(es_body))))
            return True
    
    def delete_one_doc(self, es_index, es_type, es_id) :
        res = self.es_client.delete(es_index, es_type, es_id)
        if "_shards" not in res or "successful" not in res["_shards"] or 0 >= int(res["_shards"]["successful"]) :
            print("delete failed, index[%s] type[%s] id[%s]" %(es_index, es_type, es_id))
        else :
            print("delete successful, index[%s] type[%s] id[%s]" %(es_index, es_type, es_id))
        return

    def find_same_doc(self, es_index, es_id):
        es_body = {}
        es_body["query"] = {}
        es_body["query"]["match"] = {}
        es_body["query"]["match"]["_id"] = es_id
        res = self.es_client.search(index=es_index, body=es_body)
        return res
    """
        if "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]) or "_source" not in res["hits"]["hits"][0] :
            return {}
        else :
            return res["hits"]["hits"][0]["_source"]
    """

    # 查取详情, 包含_type信息
    def find_same_doc_with_type(self, es_index, es_id):
        es_body = {}
        es_body["query"] = {}
        es_body["query"]["match"] = {}
        es_body["query"]["match"]["_id"] = es_id
        res = self.es_client.search(index=es_index, body=es_body)
        #print res
        if "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]) or "_source" not in res["hits"]["hits"][0] or "_type" not in res["hits"]["hits"][0]:
            return None, {}
        else :
            return res["hits"]["hits"][0]["_type"], res["hits"]["hits"][0]["_source"]
    
    def find_doc_all(self, es_index, meta_type):
        """
        查询metatpye下面所有的内容
        """
        es_body = {}
        es_body["query"] = {}
        es_body["query"]["match"] = {}
        es_body["query"]["match"]["metaType"] = meta_type
        res = self.es_client.search(index=es_index, body=es_body)
        if "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]):
            return []
        else:
            return res["hits"]["hits"]
        
        
    def find_duplicate(self, es_index, doc_id, simhash, simhash_slices, computer):
        result = {}
        es_body = {}
        es_body["query"] = {}
        es_body["query"]["bool"] = {}
        es_body["query"]["bool"]["should"] = []
        es_body["query"]["bool"]["minimum_should_match"] = SIMHASH_SHOULD_NUM
        es_body["_source"] = ES_FIELDS
        for i in range(0, len(simhash_slices)):
            should = {}
            should["match"] = {}
            key = "simhash_" + str(i)
            value = str(simhash_slices[i])
            should["match"][key] = value
            es_body["query"]["bool"]["should"].append(should)
        # 只与库里的有效数据进行比对
        es_body["query"]["bool"]["must_not"] = []
        valid = {}
        valid["match"] = {}
        valid["match"]["valid"] = "0"
        es_body["query"]["bool"]["must_not"].append(valid)
        blocked_by = {}
        blocked_by["terms"] = {}
        blocked_by["terms"]["blocked_by"] = ["title", "simhash"]
        es_body["query"]["bool"]["must_not"].append(blocked_by)
        
        response = self.es_client.search(index = es_index, body = es_body)
        #print(es_body)
        #print(response)
        if "hits" not in response or "hits" not in response["hits"]:
            print("no hits in response, request body -> " + str(json.dumps(es_body)))
            return []
        for hit in response["hits"]["hits"]:
            if "_source" not in hit or "_index" not in hit or "_type" not in hit:
                print("no _source/_index/_type in hit -> " + str(json.dumps(hit)))
                continue
            hit_is_full = True
            for key in ES_FIELDS :
                if key not in hit["_source"] :
                    print ("key not exist -> " + str(json.dumps(hit)))
                    hit_is_full = False
                    break
            if False == hit_is_full :
                continue
            block_simhash = hit["_source"]
            dis = computer.simhash_distance(int(simhash), int(hit["_source"]["simhash"]))
            #print(dis)
            if SIMHASH_DIS_TH < dis:
                continue
            result = hit["_source"]
            result["_index"] = hit["_index"]
            result["_type"] = hit["_type"]
            print("id[%s] simhash[%s] found duplicate data[dis:%s] -> %s" % (str(doc_id), str(simhash), str(dis), str(json.dumps(result))))
            break
        return result

    def find_user_can_feel_docs(self, es_index, info):
        if "title" not in info or None == info["title"] or 15 >= len(info["title"]) :
            return False
        title = info["title"]
        result = {}
        es_body = {}
        es_body["query"] = {}
        es_body["query"]["match"] = {}
        es_body["query"]["match"]["title.keyword"] = title
        es_body["_source"] = ["title"]
        response = self.es_client.search(index = es_index, body = es_body)
        if "hits" not in response or "hits" not in response["hits"]:
            return False
        if 0 == len(response["hits"]["hits"]):
            return False
        else :
            print("id[%s] title[%s] find user can feel doc -> [%s]" % (info["id"], info["title"], response["hits"]["hits"][0]))
            return True


    def import_relevant_results(self, es_id, relevant_ids, relevant_name):
        if COUNTRY not in BUILD_COUNTRIES :
            print("import relevant results failed, available country index not found!")
            return False
        es_index = BUILD_COUNTRIES[COUNTRY]
        es_type, es_body = self.find_same_doc_with_type(es_index, es_id)
        if None == es_type or {} == es_body :
            print("import relevant results failed, detail/_type info not found -> " + es_id)
            return False
        update_body = {}
        update_body[relevant_name] = relevant_ids

        return self.update_one_doc(es_index, es_type, es_id, update_body)


def test_query_all():
    elastic_search_util = ElasticSearchUtil()
    #dd = elastic_search_util.find_doc_all("korea","1")  
    #print len(dd) 
    dd = elastic_search_util.find_same_doc("india","64b8086bffe45e174ee7820d167073dc")     
    print dd
if __name__ == '__main__':
    #elastic_search_util = ElasticSearchUtil()
    #elastic_search_util.test()
    test_query_all()
