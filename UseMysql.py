#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################################
#
# File Name:  UseMysql.py
#
# Function:   
#
# Usage:  
#
# Author: liweihua
#
# Create Time:    2016-11-02 10:18:34
#
######################################################

from datetime import datetime, timedelta
import time
import os
import sys,MySQLdb,traceback
reload(sys)
sys.setdefaultencoding("utf-8")

class mysql:  
    def __init__ (self,db):
        dbchoose={\
	'xuexizhou'  : {'host': "",  'user': '',     'passwd': '' }
        }  
        
        self.host   = dbchoose[db]['host']  
        self.user   = dbchoose[db]['user']
        self.passwd = dbchoose[db]['passwd']
        self.db     = db  
        self.port   = 3306  
        self.charset= "utf8"  
        self.conn   = None  
        self._conn()  
  
    def _conn (self):  
        try:  
            self.conn = MySQLdb.Connection(self.host, self.user, self.passwd, self.db, self.port , self.charset)  
            return True  
        except :  
            return False  
  
    def _reConn (self,num = 28800,stime = 3): #重试连接总次数为1天,这里根据实际情况自己设置,如果服务器宕机1天都没发现就......  
        _number = 0  
        _status = True  
        while _status and _number <= num:  
            try:  
                self.conn.ping()       #cping 校验连接是否异常  
                _status = False  
            except:  
                if self._conn()==True: #重新连接,成功退出  
                    _status = False  
                    break  
                _number +=1  
                time.sleep(stime)      #连接不成功,休眠3秒钟,继续循环，知道成功或重试次数结束  
  
    # 
    def query (self, sql = '', param = ()):  
        try:  
            self._reConn()  
            self.cursor = self.conn.cursor()
	    #self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)  
            self.cursor.execute (sql,param)  
            result = self.cursor.fetchall()  
            self.cursor.close ()  
            return result  
        except MySQLdb.Error,e:  
            print "Error %d: %s" % (e.args[0], e.args[1])  
            #return False  
    
    # 
    def insert (self, sql = '', param = ()):
        try:
            self._reConn()
            self.cursor = self.conn.cursor()
            #self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)  
            self.cursor.execute (sql,param)
            self.cursor.close ()
        except MySQLdb.Error,e:
            print "Error %d: %s" % (e.args[0], e.args[1])  
            #return False

    #
    def update (self, sql = '', param = ()):
        try:
            self._reConn()
            self.cursor = self.conn.cursor()
            #self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)  
            self.cursor.execute (sql,param)
            self.cursor.close ()
        except MySQLdb.Error,e:
            print "Error %d: %s" % (e.args[0], e.args[1])  
            #return False
    
    # 限制数据结果
    def select_limit (self, sql ='',offset = 0, length = 20):  
        sql = '%s limit %d , %d ;' % (sql, offset, length)  
        return self.select(sql)  
    
    # '数据库commit操作'
    def commit (self):
	self.conn.commit()
    
    # 关闭数据库连接
    def close (self):  
        self.conn.close()  

if __name__=='__main__':  
    my = mysql('haibian')  
    print my.select('select * from hb_grades;')  
    my.close()  
    print my.query('select * from hb_grades;')
    my.close()

