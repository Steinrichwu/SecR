# -*- coding: utf-8 -*-
"""
Created on Sat May 16 15:39:40 2020

@author: wudi
"""
import time
from HotStock4 import Prep
from Toolbox import DataCollect
from MSSQL import MSSQL
import pandas as pd
from Querybase import Query

P=Prep()
DC=DataCollect()
Q=Query()
#54'
def TableLefjoint():
    sql="select TradingDay, SM.SecuCode, TotalMV from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where TradingDay in ("+str(rebaldaylist)[1:-1]+") and SM.SecuCode in ("+ str(tickerlist)[1:-1]+")"
    start=time.time()
    reslist=ms.ExecNonQuery(sql)
    rechist=pd.DataFrame(reslist.fetchall())    
    end=time.time()
    print(end-start)
    return()            
#7.7
def WithTableJoin():
    sql="With QTP as (select TradingDay, TotalMV, InnerCode from JYDBBAK.dbo.QT_Performance where TradingDay in  ("+str(rebaldaylist)[1:-1]+")) "
    sql=sql+",SM as (select SecuCode,InnerCode from JYDBBAK.dbo.SecuMain where SecuCode in ("+ str(tickerlist)[1:-1]+")) "
    sql=sql+"Select QTP.TradingDay,QTP.TotalMV,SM.SecuCode from QTP left join SM on QTP.InnerCode=SM.InnerCode"
    start=time.time()
    reslist=ms.ExecNonQuery(sql)
    rechist=pd.DataFrame(reslist.fetchall())
    end=time.time()
    print(end-start)
    return()

def MultipleQury():
    sql1="select TradingDay, TotalMV, InnerCode from JYDBBAK.dbo.QT_Performance where TradingDay in  ('2018-12-25') and InnerCode='311'"
    sql2="select TradingDay, TotalMV, InnerCode from JYDBBAK.dbo.QT_Performance where TradingDay in  ('2020-04-15') and InnerCode='311'"
    sqllist=sql1+'; '+sql2
    a=pd.DataFrame(ms.ExecQuery(sqllist))
    return(a)
    
def WithTable():
    sql=[]
    for rebalday in rebaldaylist:
        tickerlist=rechist.loc[rechist['rebalday']==rebalday,'ticker'].tolist()
        sqlpart="With QTP as (select TradingDay, TotalMV, InnerCode from JYDBBAK.dbo.QT_Performance where TradingDay ='"+rebalday+"' )"
        sqlpart=sqlpart+", SM as (select SecuCode,InnerCode from JYDBBAK.dbo.SecuMain where SecuCode in ("+ str(tickerlist)[1:-1]+")) "
        sqlpart=sqlpart+"Select QTP.TradingDay,SM.SecuCode,QTP.TotalMV from SM left join QTP on QTP.InnerCode=SM.InnerCode where TradingDay='"+rebalday+"'"
        sql.append(sqlpart)
    start=time.time()    
    reslist=ms.ExecListQuery(sql)
    end=time.time()
    mcaptab=pd.DataFrame(reslist,columns=['date','ticker','mcap'])
    print(end-start)
    return(mcaptab)

def SQLList_Deq():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    startdate='2010-12-28'
    rebal_period=20
    rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
    signal='PE'
    sql=[]
    print('startnow')
    start=time.time()
    for rebalday in rebaldaylist:
        sqlpart=getattr(Q,'Valuation_Reciprocal')(signal,rebalday)
        sql.append(sqlpart)
    reslist=ms.ExecDeqQuery(sql)
    end=time.time()
    print(end-start)
    a=pd.DataFrame(reslist)
    return(a)
    
#1. convert data in list, append list is way faster than append dataframe
#2. use dict to save data, use pd.DataFrame.from_dict to convert to dataframe instead of adding rows
#Cut from 2.2 seconds to 0.4 seconds
def convertditc_dataframe(rebaldaylist,tickerlist):
    start=time.time()
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    query="SELECT LEI.CancelDate,SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=3 and SM.SecuCategory=1 and SM.ListedState = 1 and SM.ListedSector in (1,2,6)"
    reslist=ms.ExecQuery(query)
    tickersectors=pd.DataFrame(reslist,columns=['date','ticker','primecode'])
    tickersectors['date']=tickersectors['date'].astype(str)
    tempdict={}
    for rebalday in rebaldaylist:
        tempdict[rebalday]=tickerlist
    df=pd.DataFrame.from_dict(tempdict)
    rebalday_ticker=pd.melt(df,value_vars=list(df.columns),value_name='ticker',var_name='date')
    rebalday_ticker['primecode']=np.nan
    stock_sector=rebalday_ticker.append(tickersectors)
    stock_sector=stock_sector.sort_values(by=['ticker','date'],ascending=[True,True])
    stock_sector=stock_sector.reset_index(drop=True)
    stock_sector['primecode']=stock_sector['primecode'].fillna(method='bfill')
    stock_sector=stock_sector.loc[stock_sector['date'].isin(rebaldaylist)]
    end=time.time()
    print(end-start)
    return(stock_sector)
    
def teststock_sector(rebaldaylist,tickerlist):
    start=time.time()
    a=DC.Stock_sector(rebaldaylist,tickerlist)
    end=time.time()
    print(end-start)
    return(a)