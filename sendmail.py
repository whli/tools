#!/usr/bin/python
# -*- coding: UTF-8 -*-
 
import smtplib
from email.mime.text import MIMEText
from email.header import Header
 

def send_text(msg, sub):

    sender = 'from@runoob.com'
    receivers = ['weihua.li@zenjoy.net']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
     
    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(msg, 'plain', 'utf-8')
    #message['From'] = Header("Zenjoy", 'utf-8')
    #message['To'] =  Header("DataUser", 'utf-8')
     
    subject = sub
    message['Subject'] = Header(subject, 'utf-8')
     
     
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message.as_string())
        print "邮件发送成功"
    except smtplib.SMTPException:
        print "Error: 无法发送邮件"


def send_html(msg, sub):

    sender = 'from@runoob.com'
    receivers = ['weihua.li@zenjoy.net']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
      
    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(msg, 'html', 'utf-8')
    #message['From'] = Header("Zenjoy", 'utf-8')
    #message['To'] =  Header("DataUser", 'utf-8')
      
    subject = sub 
    message['Subject'] = Header(subject, 'utf-8')
      
      
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message.as_string())
        print "邮件发送成功"
    except smtplib.SMTPException:
        print "Error: 无法发送邮件"


if __name__ == "__main__":

    subject = "Python SMTP Test"
    msg_text = "Text Test"
    msg_html = """
                <p>Python 邮件发送测试...</p>
                <p><a href="http://www.runoob.com">这是一个链接</a></p>
               """

    send_text(msg_text, subject)
    send_html(msg_html, subject)
