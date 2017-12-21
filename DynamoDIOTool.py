#-*-coding:utf8-*-
"""
author :zhoushuo
date:20170204
brief : 用于DetailInfoOnline表中工具类
"""

from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import datetime
import json
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

class DetailInfoOnline(object):

    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name) #first for test

    def query_item(self, docid):
        """
        query 
        docid 为唯一id
        """
        try:
            response = self._table.query(
                    KeyConditionExpression=Key('id').eq(docid)
                    )  
            if len(response) > 0:
                return response['Items']
            else:
                return []
        except Exception,e:
            print (str(e))
            return []

    def query_mul_item(self, docid_list):
        """
        query
        查询docid_list中所有docid的信息
        """
        try:
            fe = Attr("id").is_in(docid_list)
            response = self._table.scan(
                FilterExpression=fe
                )
            result = []
            while "Items" in response and 0 < len(response["Items"]):
                if 0 != len(response["Items"]):
                    result += response["Items"]
                if "LastEvaluatedKey" not in response:
                    break
                lek = response['LastEvaluatedKey']
                response = self._table.scan(
                        FilterExpression=fe,
                        ExclusiveStartKey=lek
                )
            return result
        except Exception,e:
            print(str(e))
            return []                
            
if __name__ =="__main__":
    print("hi")
    data_obj = DetailInfoOnline('dynamodb', '', "", 'DetailInfoOnline')
    res = data_obj.query_mul_item(["29e729540a74587f5ac3833fdfdf9203", "8c125ce94471eead81ed321c629bcf22"])
    print(len(res))
    print(type(res[0]))
    print(res[0])
    print(res[1])
