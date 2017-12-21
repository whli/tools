#-*-coding:utf8-*-

from __future__ import print_function # Python 2/3 compatibility
import sys
import boto3
import json
import decimal
import datetime
import logging
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

class SQSUtil(object):
    def __init__(self, REGION, SQS_END_URL, QUEUE_NAME):
        #sqs 相关
        self.sqs = boto3.resource('sqs', region_name=REGION, endpoint_url=SQS_END_URL)
        self.queue = self.sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    # 从消息队列中接收消息
    def receive(self) :
        results = []
        messages = self.queue.receive_messages(MaxNumberOfMessages=10)
        for message in messages :
            #body = json.loads(message.body, cls=DecimalEncoder)
            body = json.loads(message.body)
            message.delete()
            results.append(body)
        time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print (time_str + " receive " + str(len(results)) + " messages")
        return results


if __name__ == '__main__':
    sqs_util = SQSUtil("","","")
    results = sqs_util.receive()
    print (results[0].keys())
