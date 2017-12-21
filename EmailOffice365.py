#!/usr/bin/env python
#coding: utf-8

import smtplib
import email
import mimetypes
import json
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText

mail_host = ""
mail_user = ""
mail_pwd = ""
mail_postfix = ""

def sendmail(to_list,subject,content):
  # translation
    me = mail_user+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEMultipart('related')
    msg['Subject'] = email.Header.Header(subject,'utf-8')
    msg['From'] = mail_user
    msg['To'] = ";".join(to_list)
    msg.preamble = 'This is a multi-part message in MIME format.'
    msgAlternative = MIMEMultipart('alternative')
    msgText = MIMEText(content, 'plain', 'utf-8')
    msgAlternative.attach(msgText)
    msg.attach(msgAlternative)
    try:
        s = smtplib.SMTP_SSL()
        s.connect(mail_host, 587)
        s.starttls()
        s.login(mail_user,mail_pwd)
        s.sendmail(mail_user, to_list, msg.as_string())
        s.quit() 
    except Exception,e:
        print e
        return False
    return True
    
def sendhtmlmail(to_list,subject,content):
  # translation
    me = mail_user+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEMultipart('related')
    msg['Subject'] = email.Header.Header(subject,'utf-8')
    msg['From'] = mail_user
    msg['To'] = ";".join(to_list)
    msg.preamble = 'This is a multi-part message in MIME format.'
    msgAlternative = MIMEMultipart('alternative')
    msgText = MIMEText(content, 'html', 'utf-8')
    msgAlternative.attach(msgText)
    msg.attach(msgAlternative)
    try:
        print dir(smtplib)
        s = smtplib.SMTP()
        s.connect(mail_host, 587)
        s.starttls()
        s.login(mail_user,mail_pwd)
        s.sendmail(mail_user, to_list, msg.as_string())
        s.quit() 
    except Exception,e:
        print e
        return False
    return True

if __name__ == '__main__':

    detail = """
    测试一下邮件看看
    """

    if sendhtmlmail(["wenhai.pan@zenjoy.net"],"测试邮件", detail):
        print "Success!"
    else:
        print "Fail!"

