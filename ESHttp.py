#-*-coding:utf8-*-
import sys
import json
import urllib2
import urllib

sys.path.append("../conf")
from config import *

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

class HttpElasticSearch(object):
    def __init__(self, url):
        self.url = url
    
    def search_one_id(self, id_in):
        """
        从es查某id
        """
        fix_url = self.url + "_search"
        data ={"query":{}}
        data["query"]["match"] = {}
        data["query"]["match"]["id"] = id_in
        data = json.dumps(data)
        req = urllib2.Request(fix_url, data)
        req.get_method = lambda:'POST'
        out = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
        return out

    def import_one_doc(self, se_type, se_id, es_body):
        """ 
        向es插入一条数据
        body 是个dict
        """
        new_body = json.dumps(es_body, cls=DecimalToStringEncoder)
        fix_url = self.url + se_type + "/" + se_id 
        req = urllib2.Request(fix_url, new_body)
        req.get_method = lambda:'PUT'
        res = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
        if "_shards" not in res or "successful" not in res["_shards"] or 0 >= int(res["_shards"]["successful"]) :
            print("import failed, index[%s] type[%s] id[%s] body[%s]" %(fix_url, se_type, se_id, new_body))
        else :
            print("import successful, index[%s] type[%s] id[%s]" %(fix_url, se_type, se_id))
        return

    def search_id_field(self, id_in, field):
        """查询id的某个字段值
        Args:
            id_in:str
            field:str 要查询的字段名称
        Return:
            source:list 字段取值
        """
        fix_url = self.url + "_search"
        data ={"query":{}, "_source":field}
        data["query"]["match"] = {}
        data["query"]["match"]["id"] = id_in
        data = json.dumps(data)
        req = urllib2.Request(fix_url, data)
        req.get_method = lambda:'POST'
        try:
            out = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
            source = out["hits"]["hits"][0]["_source"][field]
            if isinstance(source, list):
                return source
            else:
                return [source]
        except Exception, e:
            print str(e)
            return []

    def search_id_multi_field(self, id_in, field_list):
        """查询id的某个字段值
        Args:
            id_in:str
            field:str 要查询的字段名称
        Return:
            source:list 字段取值
        """
        fix_url = self.url + "_search"
        data ={"query":{}, "_source":field_list}
        data["query"]["match"] = {}
        data["query"]["match"]["id"] = id_in
        data = json.dumps(data)
        req = urllib2.Request(fix_url, data)
        req.get_method = lambda:'POST'
        try:
            out = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
            source = out["hits"]["hits"][0]["_source"]
            return source
        except Exception, e:
            print str(e)
            return {}
    
    def search_title_regx(self, title_regx):
        """
        根据title的正则表达式
        查出相应的id
        """
        fix_url = self.url + "_search"
        data = {"query":{}}
        data["query"]["wildcard"] = {}
        data["query"]["wildcard"]["title"] = title_regx
        data["_source"] = ["id", "title"]
        data = json.dumps(data)
        req = urllib2.Request(fix_url, data)
        req.get_method = lambda:'POST'
        out = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
        return out
    
    def update_one_doc(self, id_in, update_body):
        """
        update_body is a dict:
        like {"viewCount":"5800"}
        """
        res = self.search_one_id(id_in)
        if "hits" not in res or "hits" not in res["hits"] or "_type" not in res["hits"]["hits"][0]:
            return False 
        type_t = str(res["hits"]["hits"][0]["_type"])
        fix_url = self.url + type_t + "/" + id_in + "/_update"    
        data = {"doc":update_body}
        data = json.dumps(data)
        req = urllib2.Request(fix_url, data)
        req.get_method = lambda:'POST'
        out = json.loads(urllib2.urlopen(req, timeout=1000).read().strip())
        #if "_shards" not in out or int(out["_shards"]["successful"]) != int(out["_shards"]["total"]):
        #    return False
        return

http_es_user = HttpElasticSearch(es_url_user)

if __name__ == "__main__":
    obj = HttpElasticSearch(es_url_user)
    res = obj.search_id_multi_field("6bf8e3c87e9472bb33b903051058705a", ["channel", "topic_list"])
    print res
