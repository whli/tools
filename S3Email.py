#-*-coding:utf8-*-

import sys
import boto3

class email_send(object):

    def __init__(self, s3_link, subject):
        self.s3_link = s3_link #s3的链接， 可以拼多个
        self.subject = subject

    def send_out_email(self):
        client = boto3.client('sns', region_name='ap-northeast-1')
        response = client.publish(
        TopicArn = "arn:aws:sns:ap-northeast-1:715749150787:ad_statistics",
        Message = self.s3_link,
        Subject = self.subject,
     )   
        return response

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


if __name__ == "__main__":
    argv_len = len(sys.argv)
    if argv_len == 2:
        send_mail(sys.argv[1])
    elif argv_len == 3:
        send_mail(sys.argv[1], sys.argv[2])
    else:
        print 'Usage python warning_mail.py ["message"] ["subject"]'
        exit(1)
