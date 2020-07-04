# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 22:08:39 2019

@author: wudi
"""

import quandl
import pandas as pd
import numpy as np
import os
import math
quandl.ApiConfig.api_key = "u4Px9HqqwvMs7cdHS2u9"


import pymssql
#from SQLConn import MSSQL  
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="cp936")

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
        #self.conn.commit()
        #self.conn.close()
    
    def Commit(self):
        self.conn.commit()
   
    def CloseConnect(self):
        self.conn.close()

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="USQuant") #This is PROD  

def Download():
    sharadar=pd.read_csv("U:/UQuant/Sharadar_Tickers.csv")
    tickers=sharadar.loc[(sharadar['scalemarketcap']=='6 - Mega')|(sharadar['scalemarketcap']=='5 - Large')|(sharadar['scalemarketcap']=='4 - Mid')|(sharadar['scalemarketcap']=='3 - Small'),'ticker']
    tickers=tickers.reset_index(drop=True)
    for i in range(0,tickers.shape[0]):
        quandltic=tickers[i]
        print(quandltic+" "+str(i))
        shsdata=quandl.get_table('SHARADAR/SF1', ticker=quandltic)
        if shsdata.shape[0]>=0:
            ticker=quandltic.replace(".","_")
            shsdata.to_csv('U:/UQUant/Data/'+ticker+'.csv',index=False)

def Price_Download():
    sharadar=pd.read_csv("U:/UQuant/Sharadar_Tickers.csv")
    tickers=sharadar.loc[(sharadar['scalemarketcap']=='6 - Mega')|(sharadar['scalemarketcap']=='5 - Large')|(sharadar['scalemarketcap']=='4 - Mid')|(sharadar['scalemarketcap']=='3 - Small'),'ticker']
    tickers=tickers.reset_index(drop=True)
    for i in range(0,tickers.shape[0]):
        quandltic=tickers[i]
        print(quandltic+" "+str(i))
        shsdata=quandl.get_table('SHARADAR/SEP', ticker=quandltic)
        if shsdata.shape[0]>=0:
            ticker=quandltic.replace(".","_")
            shsdata.to_csv('U:/UQUant/Price/'+ticker+'.csv',index=False)



def uploader(quandltic,identifier):
    sec_ind=pd.read_csv("U:/UQuant/Data/"+quandltic)
    for i in range (0,sec_ind.shape[0]):  
        sql="insert into USQuant.dbo.Quandl Values ("
        sql=sql+str(identifier)+', '
        for j in range (0,sec_ind.shape[1]):
            if (np.isreal(sec_ind.iloc[i,j])==True):
                if(math.isnan(sec_ind.iloc[i,j])==True):
                    sql=sql+str('null, ')
                else:
                    sql=sql+str(sec_ind.iloc[i,j])+", "
            else:
                sql=sql+"'"+sec_ind.iloc[i,j]+"', "
        identifier=identifier+1
        sql=sql[:-2]
        sql=sql+")"
        ms.ExecNonQuery(sql)
    ms.Commit()   
    return(identifier)
    
def px_uploader(quandltic,identifier):
    sec_ind=pd.read_csv("U:/UQuant/Price/"+quandltic)
    for i in range (0,sec_ind.shape[0]):  
        sql="insert into USQuant.dbo.Price Values ("
        sql=sql+str(identifier)+', '
        for j in range (0,sec_ind.shape[1]):
            if (np.isreal(sec_ind.iloc[i,j])==True):
                if(math.isnan(sec_ind.iloc[i,j])==True):
                    sql=sql+str('null, ')
                else:
                    sql=sql+str(sec_ind.iloc[i,j])+", "
            else:
                sql=sql+"'"+sec_ind.iloc[i,j]+"', "
        identifier=identifier+1
        sql=sql[:-2]
        sql=sql+")"
        ms.ExecNonQuery(sql)
    ms.Commit()   
    return(identifier)

def Tickers_uploader():
    sec_ind=pd.read_csv("U:/UQuant/Sharadar_Tickers.csv")
    for i in range (0,sec_ind.shape[0]):  
        print(i)
        sql="insert into USQuant.dbo.Tickers Values ("
        sql=sql+str(i)+', '
        for j in range (0,sec_ind.shape[1]):
              if (np.isreal(sec_ind.iloc[i,j])==True):
                if(math.isnan(sec_ind.iloc[i,j])==True):
                    sql=sql+str('null, ')  
                else:
                    sql=sql+str(sec_ind.iloc[i,j])+","
              else:
                sql=sql+"'"+sec_ind.iloc[i,j]+"', "
        sql=sql[:-2]
        sql=sql+")"
        ms.ExecNonQuery(sql)
    ms.Commit()   
    ms.CloseConnect()
    
def batch_upload():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="USQuant") #This is PROD  
    filelist=os.listdir('U:\UQuant\Price')
    identifier=0
    for quandltic in filelist:
        print(quandltic)
        identifier=px_uploader(quandltic,identifier)
    ms.CloseConnect()
    

def Growth_uploader(quandltic,identifier):
    sec_ind=pd.read_csv("U:/UQuant/Growth/"+quandltic)
    for i in range (0,sec_ind.shape[0]):  
        sql="insert into USQuant.dbo.Growth Values ("
        sql=sql+str(identifier)+', '
        for j in range (0,sec_ind.shape[1]):
            if (np.isreal(sec_ind.iloc[i,j])==True):
                if(math.isnan(sec_ind.iloc[i,j])==True):
                    sql=sql+str('null, ')
                else:
                    sql=sql+str(sec_ind.iloc[i,j])+", "
            else:
                sql=sql+"'"+sec_ind.iloc[i,j]+"', "
        identifier=identifier+1
        sql=sql[:-2]
        sql=sql+")"
        ms.ExecNonQuery(sql)
    ms.Commit()   
    return(identifier)

def Growth_batch_upload():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="USQuant") #This is PROD  
    filelist=os.listdir('U:\UQuant\Growth')
    identifier=0
    for quandltic in filelist:
        print(quandltic)
        identifier=Growth_uploader(quandltic,identifier)
    ms.CloseConnect()


#Loop through the price file and get Return data of each stock
def Return_uploader(quandltic,identifier):
    sec_ind=pd.read_csv("U:/UQuant/Return/"+quandltic)
    for i in range (0,sec_ind.shape[0]):  
        sql="insert into USQuant.dbo.DTDReturn Values ("
        sql=sql+str(identifier)+', '
        for j in range (0,sec_ind.shape[1]):
            if (np.isreal(sec_ind.iloc[i,j])==True):
                if(math.isnan(sec_ind.iloc[i,j])==True):
                    sql=sql+str('null, ')
                else:
                    sql=sql+str(sec_ind.iloc[i,j])+", "
            else:
                sql=sql+"'"+sec_ind.iloc[i,j]+"', "
        identifier=identifier+1
        sql=sql[:-2]
        sql=sql+")"
        ms.ExecNonQuery(sql)
    ms.Commit()
    return(identifier)

def Return_batch_upload():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="USQuant") #This is PROD  
    filelist=os.listdir('U:\UQuant\Return')
    identifier=0
    for quandltic in filelist:
        print(quandltic)
        identifier=Return_uploader(quandltic,identifier)
    ms.CloseConnect()