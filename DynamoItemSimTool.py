#-*-coding:utf8-*-
"""
author : zhoushuo
brief : 用于MXBetaItemSim表中工具类
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


class ImportSimData(object):

    def __init__(self, table_kind, region_name, endpoint_url, table_name):
        self._dynamodb = boto3.resource(table_kind, region_name = region_name, endpoint_url = endpoint_url)
        self._table = self._dynamodb.Table(table_name)

    def import_one_item(self, doc_id, detail_info):
        #time_stamp = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"))
        try :
            response=self._table.put_item(
                    Item={
                    'docId': doc_id,
                    'SimInfo': detail_info
                    }
                )
            return True
        except Exception, e :
            print(str(e))
            return False

    def query_item(self, doc_id):
        """
        query 
        """
        try:
            response = self._table.query(
                    KeyConditionExpression=Key('docId').eq(doc_id)
                    )  
            if len(response) > 0:
                return response['Items']
            else:
                return []
        except Exception,e:
            print (str(e))
            return []
    
    def query_mul_item(self, doc_id_list):
        """
        query 
        """
        try:
            #doc_id_str = ",".join(doc_id_list)
            #fe = Key('docId').between(doc_id_str)
            fe = Attr('docId').is_in(doc_id_list)
            response = self._table.scan(
                    FilterExpression = fe
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
                    ExclusiveStartKey=lek
                )
            
            return result      
        except Exception,e:
            print (str(e))
            return []
  
    def delete_one_item(self, doc_id):
        """
        删除一个item
        """
        fe = Key('docId').eq(int(doc_id))
        #pe = "#sign, time_stamp"
        #ean = { "#sign": "url_sign"}
        esk = None

        response = self._table.scan(
            FilterExpression=fe,
            #ProjectionExpression=pe,
            #ExpressionAttributeNames=ean
        )
       
        for i in response['Items']:
            print (str(i))
            if "docId" not in i :
                continue
            doc_id = i["docId"]
            
            try :
                del_response = self._table.delete_item(
                    Key={
                        'docId': doc_id,
                    }
                )

            except ClientError as e:
                print (str(e))
                #if e.del_response['Error']['Code'] == "ConditionalCheckFailedException":
                #    print(e.del_response['Error']['Message'])
                #else:
                #    raise
                #    print("clear not successful")
        return


    def scan_all(self):
        """
        得到表中 time_stamp为 （start，end） 之间的数据
        """ 
        try: 
      
            #pe = "url_sign, time_stamp, info" #test
            pe = "docId" #real
            response = self._table.scan(
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
                    ProjectionExpression=pe,
                        ExclusiveStartKey=lek
                )
            
            return result   
        
        except Exception,e:
            print(str(e))
            return []                
   
    def clear_table(self):
        pe = "docId"
        #ean = { "#sign": "url_sign"}
        esk = None

        response = self._table.scan(
            ProjectionExpression=pe,
            #$ExpressionAttributeNames=ean
        )
        #return response['Items']
        result = self.scan_all()
        for i in result:
            if "docId" not in i :
                continue
            doc_id = i["docId"]
            try :
                del_response = self._table.delete_item(
                    Key={
                        'docId':doc_id
                    },
                )

            except ClientError as e:
                if e.del_response['Error']['Code'] == "ConditionalCheckFailedException":
                    print(e.del_response['Error']['Message'])
                else:
                    raise
                    print("clear not successful")
        return
    def test(self):
        info = {}
        info["info"] = "nothing here, haha!"
        url_sign = 2

        info["simhash"] = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        #self.import_one_item(url_sign, info)
        #self.delete_one_item(url_sign)
        self.clear_table()
        return


if __name__ =="__main__":
    pass
