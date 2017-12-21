#coding=utf-8

import sys
import os
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

class ElasticSearchUtil:
    def __init__(self):
        self.es_client = Elasticsearch(
            hosts=[{'host': ES_ENDPOINT, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            timeout=600,
            connection_class=RequestsHttpConnection
        )

    def search_by_publishtime(self, es_index, scroll='10h') :
        es_body = {}
        es_body["size"] = 1000
        es_body["query"] = {}
        es_body["query"]["match_all"] = {}
        es_body["_source"] = ["selfViewCount", "id", "selfClickViewRatio","selfClickCount"]

        res = self.es_client.search(index=es_index, body=es_body, scroll=scroll)
        if "_scroll_id" not in res or "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]) or "_source" not in res["hits"]["hits"][0] :
            return None, []
        else :
            return res["_scroll_id"], res["hits"]["hits"]

    def scroll_search_by_publishtime(self, scroll_id, scroll='10h') :
        res = self.es_client.scroll(scroll_id=scroll_id, scroll=scroll)
        if "hits" not in res or "hits" not in res["hits"] or 0 >= len(res["hits"]["hits"]) or "_source" not in res["hits"]["hits"][0] :
            return None, []
        else :
            return scroll_id, res["hits"]["hits"]

if __name__ =="__main__":
	es_client = ElasticSearchUtil()
	scroll_id,out = es_client.search_by_publishtime("mx-beta","1m")
	res_list = out
	while True:
		scroll_id,out = es_client.scroll_search_by_publishtime(scroll_id,"1m")
		if out:
			res_list += out
		else:
			break
	for item in res_list:
		source = item.get("_source",{})
		selfClickViewRatio = source.get("selfClickViewRatio","0")
		selfViewCount = source.get("selfViewCount","0")
		video_id = source.get("id","")
		selfClickCount = source.get("selfClickCount","0")
		print video_id,selfViewCount,selfClickViewRatio,selfClickCount
