#-*-coding:utf8-*-
"""
author : wangyue
date:20121220
brief : 用于UniqueCrawledData表中工具类
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

class UniqueCrawledData(object):

    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name) #first for test

    def query_item(self, url_sign):
        """
        query 
        url_sign 为唯一id
        """
        try:
            response = self._table.query(
                    KeyConditionExpression=Key('simhash').eq(url_sign)
                    )  
            if len(response) > 0:
                return response['Items']
            else:
                return []
        except Exception,e:
            print (str(e))
            return []

    def query_item_detail(self, url_sign , key_name):
        """
        """
        try:
            response = self._table.query(
                    KeyConditionExpression=Key(key_name).eq(url_sign)
                    )  
            if len(response) > 0:
                return response['Items']
            else:
                return []
        except Exception,e:
            print (str(e))
            return []
        
    def scan_period(self, start_time, end_time):
        """
        得到表中 time_stamp为 （start，end） 之间的数据
        """ 
        try: 
      
            fe = Key("time_stamp").between(start_time, end_time)
            #pe = "url_sign, time_stamp, info" #test
            pe = "simhash, time_stamp, info" #real
            response = self._table.scan(
                    FilterExpression=fe,
                    ProjectionExpression=pe,
                    )
            result = []
            while 'Items' in response and 0 < len(response['Items']) :
            	if 0 != len(response['Items']) :
			result += response['Items']				
            	if 'LastEvaluatedKey' not in response :
                	break
            	lek = response['LastEvaluatedKey']
            	response = self._table.scan(
                	FilterExpression=fe,
                	ProjectionExpression=pe,
                        ExclusiveStartKey=lek
            	)
            
	    return result	   

        except Exception,e:
            print(str(e))
            return []  
              
    def delete_one_item(self, simhash, timestamp):
        """
        删除one item
        """
        try:
            self._table.delete_item(
            Key={
                'simhash': int(simhash),
                'time_stamp': int(timestamp)
                }
            )
        except Exception,e:
            print (str(e))

    def import_one_item(self, simhash, timestamp, info):
        """
        插入one item
        """    
        try:
            self._table.put_item(
                Item={
                        'simhash': int(simhash),
                        'time_stamp': int(timestamp),
                        'info': info
                    }
                )
        except Exception,e:
            print (str(e))

if __name__ =="__main__":
    print ("hi")
