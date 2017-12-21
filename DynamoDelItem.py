#-*-coding:utf8-*-

from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
import datetime

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# del item from Dynamodb for docid between a and b
class DeleteFromDynamodb(object):
    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name) #first for test

    def del_data_item(self, docId, beginnum = 1, intervalnum = 10):
        for i in range(beginnum, beginnum + intervalnum):
            delday = datetime.datetime.now() + datetime.timedelta(days = -i)
            delday = int(delday.strftime("%Y%m%d"))
            try:
                response = self._table.delete_item(
                    Key={
                        'docId':docId,
                        'date':delday
                    }
                )
            except Exception,e:
                print(str(e))

    def del_data_list(self, docList, beginnum = 1, intervalnum = 10):
        for docId in docList:
            for i in range(beginnum, beginnum + intervalnum):
                delday = datetime.datetime.now() + datetime.timedelta(days = -i) 
                delday = int(delday.strftime("%Y%m%d"))
                try:
                    response = self._table.delete_item(
                        Key={
                            'docId':docId,
                            'date':delday
                        }   
                    )   
                except Exception,e:
                    print(str(e))

if __name__ == "__main__":
    delobj = DeleteFromDynamodb('dynamodb', '', "", 'TopHot')
    docId = "a61a614ff936d125c17939553a8aa6a2"
    #delobj.del_data_item(docId,30)
    docList = ["a61a614ff936d125c17939553a8aa6a2","1aa2687b63822f213d9ad274b858166e"]
    delobj.del_data_list(docList,30)
