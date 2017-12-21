#coding=utf-8

import os
import sys
import boto3
reload(sys)
sys.setdefaultencoding("utf-8")

class email_send(object):

    def __init__(self, s3_link, subject):
        self.s3_link = s3_link #s3的链接， 可以拼多个
        self.subject = subject

    def send_warn_email(self):
        """ 
        报警邮件
        """
        client = boto3.client('sns', region_name='ap-northeast-1')
        response = client.publish(
        TopicArn = "arn:aws:sns:ap-northeast-1:715749150787:test",
        Message = self.s3_link,
        Subject = self.subject,
     )   
        return response

def send_mail(msg="warning message", sub="warning email"):
    ''' 
    发送报警邮件
    '''
    mailobj = email_send(msg, sub)
    mailobj.send_warn_email()

def get_used_thread(monitor,monitor_file):
    result = os.system("df -h | grep %s > %s" %(monitor,monitor_file))
    with open(monitor_file) as f:
        for line in f:
            ary = line.strip().split()
            use_percent = int(ary[4].strip("%"))
            available = ary[3]
            key = ary[5]
            if use_percent >= 90:
                print key,available
                message = "the device of %s is used over 90 per, the available space is %s !" %(key,available)
                sub = "Monitor for device on ec2-13-126-49-11.ap-south-1.compute.amazonaws.com : new cluster for mengmai"
                send_mail(message,sub)
            else:
                pass

if __name__ == "__main__":
    monitor = "/mnt"
    monitor_file = "monitor_file"
    get_used_thread(monitor,monitor_file)
