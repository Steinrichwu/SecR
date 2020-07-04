# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 21:08:30 2018

@author: wudi
"""


"""
Created on Wed Jul 18 21:16:16 2018

@author: wudi
"""
import datetime
import pymssql
import math
import sys
import collections

import importlib
importlib.reload(sys)

class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        #self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="gb2312")

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"?????????")        
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"???????")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
        #???????????
        #self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        return cur
        #self.conn.commit()
        #self.conn.close()
    
    def Commit(self):
        self.conn.commit()
   
    def CloseConnect(self):
        self.conn.close()
    
    def ExecListQuery(self,sqllist):
        output=[]
        cur = self.__GetConnect()
        for sql in sqllist:
            cur.execute(sql)
            resList = cur.fetchall()
            output.extend(resList)
        return output
    
    def ExecDeqQuery(self,sqllist):
        output=collections.deque()
        cur = self.__GetConnect()
        for sql in sqllist:
            cur.execute(sql)
            resList = cur.fetchall()
            output.extend(resList)
        return output
    
print (sys.getdefaultencoding())

