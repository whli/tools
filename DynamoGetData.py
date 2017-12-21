#-*-coding:utf8-*-

from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# get data from dynamodb between a and b
class GetDataDynamo(object):
    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name)

    def get_data(self, beginday, endday):
        fe = Key('date').between(beginday, endday)
        response = self._table.scan(
            FilterExpression=fe
        )    
        
        doc_dict = {}
        for i in response['Items']:
            doc_dict.setdefault(i['docId'],[])
            doc_dict[i['docId']].append(i['date'])

        # 持续获取数据，直到最后一页
        while 'LastEvaluatedKey' in response:
            response = self._table.scan(
                FilterExpression=fe,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for i in response['Items']:
                doc_dict.setdefault(i['docId'],[])
                doc_dict[i['docId']].append(i['date'])
        return doc_dict           


if __name__ == "__main__":
    
    # 传入的两个日期必须是int型   
    beginday = 20170515
    endday = 20170515
    getobj = GetDataDynamo('dynamodb',"","","")
    doc_dict = getobj.get_data(beginday, endday)
    # 
    print("******")
    print("From %s to %s" %(beginday, endday),"热门推荐视频数：",len(doc_dict))
    print("******")
    for key in doc_dict:
        print(key,len(doc_dict[key]))
