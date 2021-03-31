# -*- coding: utf-8 -*-
"""
Created on Sun May 17 14:07:13 2020

@author: wudi
"""


#Provide tools that are often used by Niu2 or any other Ashs backtest tool

import warnings
import pandas as pd
import numpy as np
from MSSQL import MSSQL     #The SQL connection management tool
from dateutil.relativedelta import relativedelta
from Querybase import Query
from scipy.stats import mstats
import statsmodels.api as sm
from scipy.optimize import minimize
import Optimize as Opt
from scipy import stats
from sklearn.linear_model import LinearRegression

Q=Query()

tradingday=pd.read_csv("D:/SecR/Tradingday.csv")

class DataCollect():
    def __init__(self):
        pass


#Return all stocks' FIRST/SECOND sector information on rebaldays #Set first_second as second, will download the 中信二级行业
    def SectorPrep(self,rebaldaylist,publisher,first_second):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if (publisher=='CITIC')&(first_second=='first'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        if (publisher=='CSI')&(first_second=='first'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=28 and SM.SecuCategory=1"
        if (publisher=='CITIC')&(first_second=='second'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,SecondIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        tickersectors=pd.DataFrame(reslist,columns=['date','ticker','primecode'])
        if (publisher=='CITIC')&(first_second=='first'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_STIBExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        if (publisher=='CSI')&(first_second=='first'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_STIBExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=28 and SM.SecuCategory=1"
        if (publisher=='CITIC')&(first_second=='second'):
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,SecondIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_STIBExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        tickersectors2=pd.DataFrame(reslist,columns=['date','ticker','primecode'])
        tickersectors=tickersectors.append(tickersectors2)
        tempdict={}
        newtickerlist=tickersectors['ticker'].unique().tolist()
        tempdict={rebalday:newtickerlist for rebalday in rebaldaylist}
        df=pd.DataFrame.from_dict(tempdict)
        rebalday_ticker=pd.melt(df,value_vars=list(df.columns),value_name='ticker',var_name='date')
        rebalday_ticker['primecode']=np.nan
        sector=rebalday_ticker.append(tickersectors)
        sector=sector.sort_values(by=['ticker','date'],ascending=[True,True])
        sector=sector.reset_index(drop=True)
        sector['primecode']=sector['primecode'].fillna(method='bfill')
        return(sector)
    
    #Extract sector index innercode, sector secucode and sector name from database for future use
    def Sector_Index(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="select distinct SM.InnerCode,SM.SecuCode,SM.ChiNameAbbr from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where TradingDay>'2008-12-31' and secucode in ('000951','000932','000935','000936','000931','000933','000929','000930','000928','000849','000952','000937','000300') order by SecuCode ASC"
        reslist=ms.ExecQuery(sql)
        sectorinfo=pd.DataFrame(reslist,columns=['innercode','secucode','name'])
        sectorinfo['name']=[x.encode('latin-1').decode('gbk') for x in sectorinfo['name']]
        return(sectorinfo)
    
    def Sector_Mixed_Index_memb(self,sectorcode,membstartdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sector_indexinfo=pd.read_csv("D:/SecR/CSI_Index.csv")
        innercode=list(sector_indexinfo.loc[sector_indexinfo['sector']==sectorcode,'InnerCode'])[0]
        sql="select EndDate,SM.SecuCode,weight from JYDBBAK.dbo.LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='"+str(innercode)+"' and ENDDate>'"+membstartdate+"'"
        reslist=ms.ExecQuery(sql)
        tickersectors2=pd.DataFrame(reslist,columns=['date','ticker','weight'])
        return(tickersectors2)
        
#Enter rebaldaylist,primecodelist, Return stocks of the primecodes on that day
    def Sector_stock(self,rebaldaylist,primecodelist,publisher):
        sector_stock=self.SectorPrep(rebaldaylist,publisher,'first')
        sector_stock=sector_stock.loc[(sector_stock['date'].isin(rebaldaylist))&(sector_stock['primecode'].isin(primecodelist)),['date','ticker']]
        sector_stock=sector_stock.sort_values(by=['date'],ascending=[True])
        return(sector_stock)

#Enter rebaldaylist,primecodelist,Return stocks, their primecode on that day, Keeps Ashs only
    def Ashs_stock_seccode(self,rebaldaylist,primecodelist,publisher):
        sector_stock=self.SectorPrep(rebaldaylist,publisher,'first')
        sector_stock=sector_stock.loc[(sector_stock['date'].isin(rebaldaylist))&(sector_stock['primecode'].isin(primecodelist)),['date','ticker','primecode']]
        sector_stock=sector_stock.sort_values(by=['date'],ascending=[True])
        sector_stock=sector_stock.loc[sector_stock['ticker'].str[0].isin(['6','0','3'])]  
        return(sector_stock)

#Given reablday, return the primecode of all stocks on the list
    def Stock_sector(self,rebaldaylist,tickerlist,publisher):
        stock_sector=self.SectorPrep(rebaldaylist,publisher,'first')
        stock_sector=stock_sector.loc[stock_sector['date'].isin(rebaldaylist),:]
        stock_sector=stock_sector.loc[stock_sector['ticker'].isin(tickerlist),:]
        stock_sector=stock_sector.drop_duplicates()  
        return(stock_sector)
    
    def Stock_sectorCSI_CITIC(self,rebaldaylist,tickerlist):
        df=self.Stock_sector(rebaldaylist,tickerlist,'CSI')
        df=df.rename(columns={'primecode':'CSIprimecode'})
        df=self.Sector_get(df,'CITIC')
        df=df.rename(columns={'primecode':'CITICprimecode'})
        df=self.Second_sector_get(df)
        df.loc[(df['CSIprimecode']=='06')&(~df['CITICprimecode'].str[0:2].isin(['40','41','42'])),'CITICprimecode']='41'
        df.loc[df['CSIprimecode']=='06','CSIprimecode']=df.loc[df['CSIprimecode']=='06','CITICprimecode']
        df=df.rename(columns={'CSIprimecode':'primecode'})
        df=df.drop(['CITICprimecode','secondcode'],axis=1)
        return(df)
    
    #Add a column of primesector to an existing df with date and ticker  
    def Sector_get(self,df,publisher):
        daylist=list(df['date'].unique())
        tickerlist=list(df['ticker'].unique())
        stock_sector=self.Stock_sector(daylist,tickerlist,publisher)
        df['index']=df['date']+df['ticker']
        stock_sector['index']=stock_sector['date']+stock_sector['ticker']
        df=pd.merge(df,stock_sector[['index','primecode']],on='index',how='left')
        df=df.drop('index',axis=1)
        return(df)
    
    #中信二级行业 Add a column of secondsector to an existing df with datae and ticker
    def Second_sector_get(self,df):
        daylist=list(df['date'].unique())
        tickerlist=list(df['ticker'].unique())
        stock_sector=self.SectorPrep(daylist,'CITIC','second')
        stock_sector=stock_sector.loc[stock_sector['date'].isin(daylist),:]
        stock_sector=stock_sector.loc[stock_sector['ticker'].isin(tickerlist),:]
        stock_sector=stock_sector.drop_duplicates()
        stock_sector=stock_sector.rename(columns={'primecode':'secondcode'})
        stock_sector['index']=stock_sector['date']+stock_sector['ticker']
        df['index']=df['date']+df['ticker']
        df=pd.merge(df,stock_sector[['index','secondcode']],on='index',how='left')
        df=df.drop('index',axis=1)
        return(df)
        

#Return the list of seccode and secname of CITIC or CSI first level 
    def Sec_name(self,publisher):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if publisher=='CITIC':
            sql="SELECT distinct LEI.FirstIndustryCode,FirstIndustryName from JYDBBAK.dbo.LC_ExgIndustry LEI where LEI.standard=37"
        if publisher=='CSI':
            sql="SELECT distinct LEI.FirstIndustryCode,FirstIndustryName from JYDBBAK.dbo.LC_ExgIndustry LEI where LEI.standard=28"
        reslist=ms.ExecQuery(sql)
        sec_name=pd.DataFrame(reslist,columns=['sector','sectorname'])
        sec_name=sec_name.drop_duplicates(subset=['sector'],keep='first')
        sec_name['sectorname']=[x.encode('latin-1').decode('gbk') for x in sec_name['sectorname']]
        return(sec_name)
        
 #Generate a list of rebal days, to be used 
    def Rebaldaylist(self,startdate,rebal_period):
        dateloc=tradingday.loc[tradingday['date']==startdate,:].index[0]
        endloc=tradingday.shape[0]
        rebaldaylist=[]
        while dateloc+rebal_period+1<=endloc:
            rebalday=tradingday.iloc[dateloc,0]
            dateloc=dateloc+rebal_period
            rebaldaylist.append(rebalday)
        return(rebaldaylist)
    
    #Download RSI from Database
    def RSI_Db(self,rebalday):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb")
        sql="select CODE, 1/rsi_24d from BASIC_PRICE_HIS H where  TIME='"+rebalday+"'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['ticker','RSIB']
        df['enddate']=rebalday
        df['publdate']=df['enddate']
        df=df.dropna()
        df['ticker']=df['ticker'].str[0:6]
        df=df[['publdate','enddate','ticker','RSIB']]
        return(df)
    
    #Calculate RSI24 from dailyreturn on each rebalday
    def RSI24(self,dailyreturn,rebalday):
        print(rebalday)
        dateloc=tradingday.loc[tradingday['date']==rebalday,:].index[0]
        oldloc=dateloc-39                            
        rsistartdate=tradingday.iloc[oldloc,0]
        rsitab=dailyreturn.loc[(dailyreturn['date']>=rsistartdate)&(dailyreturn['date']<=rebalday),['date','ticker','closeprice']].copy() #Get the past 40tradingdays data
        tickercounts=pd.DataFrame(rsitab['ticker'].value_counts())
        tickercounts.reset_index(inplace=True)
        tickercounts.columns=['ticker','counts']             #count how many trading days were traded, excluded those have less than 24 days
        tickercounts=tickercounts.loc[tickercounts['counts']>=25,:]
        rsitab=rsitab.loc[rsitab['ticker'].isin(tickercounts['ticker']),:]
        rsitab=rsitab.sort_values(by=['ticker','date'],ascending=[True,False])
        rsitab['nthoccurence']=rsitab.groupby('ticker').cumcount()
        rsitab=rsitab.loc[rsitab['nthoccurence']<=25,:]
        rsitab['dailydiff']=rsitab['closeprice']-rsitab['closeprice'].shift(-1)
        rsitab=rsitab.loc[rsitab['nthoccurence']<24,:]
        rsitab['ve']=np.nan
        rsitab.loc[rsitab['dailydiff']>0,'ve']='_+ve'
        rsitab.loc[rsitab['dailydiff']<0,'ve']='_-ve'
        rsitab['index']=rsitab['ticker']+rsitab['ve']
        meantab=pd.DataFrame(rsitab.groupby(['index'])['dailydiff'].sum())
        meantab.reset_index(inplace=True)
        meantab['ticker']=meantab['index'].str[0:6]
        meantab['index']=meantab['index'].str[7:]
        newrsitab=pd.DataFrame(meantab.pivot_table(index='ticker',columns='index',values='dailydiff',aggfunc='first'))
        newrsitab.reset_index(inplace=True)
        newrsitab.columns=['ticker','+ve','-ve']
        newrsitab['+ve']=newrsitab['+ve']/24
        newrsitab['-ve']=newrsitab['-ve']/24
        newrsitab['RSI']=(newrsitab['+ve']/(newrsitab['+ve']-newrsitab['-ve']))*100
        newrsitab['date']=rebalday
        newrsitab=newrsitab[['date','ticker','RSI']]
        return(newrsitab)
#Stack the sql query to extract stocks of sectors in primecode list on every rebalday
#Enter rebaldaylist,primecodelist, Get the stocks beloing to those sectors in primecodelist on each rebal day
    def Sector_stock_SQL(self,rebaldaylist,primecodelist):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql=[]
        for rebalday in rebaldaylist:
            for primecode in primecodelist:
                sqlpart="SELECT '"+str(rebalday)+"' as date, C.SecuCode FROM(SELECT ROW_NUMBER() OVER (Partition by Secucode Order BY CancelDate DESC) RN2,B.*FROM("
                sqlpart=sqlpart+" Select A.SecuCode,A.Primecode, A.CancelDate from(SELECT  ROW_NUMBER() OVER (Partition by SecuCode ORDER BY LEI.CancelDate) Rn, SecuCode, FirstIndustryCode as Primecode, LEI.CancelDate from JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=3 and SM.SecuCategory=1 and  LEI.CancelDate>'"+rebalday+"') A where A.Rn=1"
                sqlpart=sqlpart+" UNION SELECT SecuCode, FirstIndustryCode as Primecode, LEI.CancelDate from JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=3 and SM.SecuCategory=1 and  LEI.CancelDate IS NULL) B) C where RN2=1 and Primecode="+str(primecode)
                sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        sectorstock=pd.DataFrame(reslist,columns=['date','ticker'])
        return(sectorstock)
    
#Download the marketcap of every selected stocks of every rebalday, using the stack method 
    def Mcap_hist(self,rebaldaylist,df):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql=[]
        for rebalday in rebaldaylist:
            tickerlist=df.loc[df['date']==rebalday,'ticker'].tolist()
            sqlpart="With QTP as (select TradingDay, NegotiableMV, InnerCode from JYDBBAK.dbo.QT_Performance where TradingDay ='"+rebalday+"' )"
            sqlpart=sqlpart+", SM as (select SecuCode,InnerCode from JYDBBAK.dbo.SecuMain where SecuCode in ("+ str(tickerlist)[1:-1]+")) "
            sqlpart=sqlpart+"Select QTP.TradingDay,SM.SecuCode,QTP.NegotiableMV from SM left join QTP on QTP.InnerCode=SM.InnerCode where TradingDay='"+rebalday+"'"
            sql.append(sqlpart)    
        reslist=ms.ExecListQuery(sql)
        mcaphist=pd.DataFrame(reslist,columns=['date','ticker','mcap'])
        mcaphist['date']=mcaphist['date'].astype(str)
        return(mcaphist)
    
    
    #With the df (date,ticker),
    def Mcap_get(self,df,dailyreturn):
        daylist=list(df['date'].unique())
        rdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(daylist),:].copy()
        df['index']=df['date']+df['ticker']
        rdailyreturn['index']=rdailyreturn['date']+rdailyreturn['ticker']
        df=pd.merge(df,rdailyreturn[['index','mcap']],on='index',how='left')
        df=df.drop(columns=['index'])
        return(df)
    
#Download the history of a benchmark and return as a dataframe
    def Benchmark_membs(self,benchmark,startdate):
        membstartdate=str(pd.to_datetime(startdate)-relativedelta(years=1))[0:10]
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if benchmark=='CSI300':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='3145'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='CSI500':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='4978'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='CSI800':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='4982'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='SuperTech':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='229190'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='SuperHealthcare':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='8890'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='SuperConDisc':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='8886'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='SuperConStap':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='8887'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        if benchmark=='CSIAll':
            sql="select EndDate,SM.SecuCode,weight from LC_IndexComponentsWeight IC left join JYDBBAK.dbo.SecuMain SM on IC.InnerCode=SM.InnerCode where IndexCode='14110'and EndDate >DATEADD(month,-3,'"+membstartdate+"')"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist)
        df.columns=['date','ticker','weight']
        df['weight']=df['weight'].astype(float)
        df['date']=df['date'].astype(str)
        df=df.sort_values(by=['date','weight'],ascending=[True,False])
        df=df.reset_index(drop=True)
        return(df)
    
    def Benchmark_return(self,benchmark,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if benchmark=='CSI300':
            sql="SELECT TradingDay,ChangePCT FROM JYDBBAK.dbo.QT_IndexQuote WHERE InnerCode = '3145' and TradingDay>'"+startdate+"'"
        if benchmark=='CSI500':
            sql="SELECT TradingDay,ChangePCT FROM JYDBBAK.dbo.QT_IndexQuote WHERE InnerCode = '4978' and TradingDay>'"+startdate+"'"
        if benchmark=='CSI800':
            sql="SELECT TradingDay,ChangePCT FROM JYDBBAK.dbo.QT_IndexQuote WHERE InnerCode = '4982' and TradingDay>'"+startdate+"'"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['date','bmdailyreturn'])
        df['date']=df['date'].astype(str)
        df['bmdailyreturn']=df['bmdailyreturn'].astype(float)
        return(df)
            
#Pull the tickers that have been recommended since 2015
    def Rec_alltickers(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    
        sql="select code from jyzb_new_1.dbo.cmb_report_research where create_date>='2004-01-01'"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['ticker']) 
        df=df[df['ticker'].apply(lambda x: len(x)==6)]
        tickerlist=df['ticker'].unique().tolist()
        return(tickerlist)        
       
#Download the ticker of NonFinancial sectors
    def Rec_NonFIGtickers(self):
        tickerlist=self.Rec_alltickers()
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    
        sql="SELECT SecuCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where (LEI.Standard=3 and FirstIndustryCode not in (40,41) and SM.SecuCategory=1 and SM.SecuCode in ("+str(tickerlist)[1:-1]+"))"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['ticker']) 
        tickerlist=df['ticker'].unique().tolist()
        return(tickerlist)
        
 #Download the dtd return of selected tickers
    def Returnhist(self,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="select convert(varchar,TradingDay, 23) as date, SM.SecuCode, ClosePrice,ChangePCT,NegotiableMV,TurnoverRateRW,TurnoverVolume from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'"+startdate+"'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['date','ticker','closeprice','dailyreturn','mcap','turnoverweek','dailyvolume']
        sql2="select convert(varchar,TradingDay, 23) as date, SM.SecuCode, ClosePrice,ChangePCT,NegotiableMV,TurnoverRateRW,TurnoverVolume from JYDBBAK.dbo.LC_STIBPerformance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'"+startdate+"'"
        reslist=ms.ExecNonQuery(sql2)
        df2=pd.DataFrame(reslist.fetchall())
        df2.columns=['date','ticker','closeprice','dailyreturn','mcap','turnoverweek','dailyvolume']
        df=df.append(df2)
        df['closeprice']=df['closeprice'].astype(float)
        df['dailyreturn']=df['dailyreturn'].astype(float)
        df['mcap']=df['mcap'].astype(float)
        df['turnoverweek']=df['turnoverweek'].astype(float)
        df['dailyvolume']=df['dailyvolume'].astype(float)
        df['dailyreturn']=df['dailyreturn']/100
        df['ticker']=df['ticker'].str.zfill(6)
        #df['ticker']=df['ticker'].apply(lambda x:x+'.SH' if x.startswith('6') else x+'.SZ')
        df['date']=df['date'].astype(str)
        return(df)


#Update the dailyreturn to the latest day and save it in the system as HDF file 全量
    def Dailyreturn_Update(self):
        dailyreturn=self.Returnhist('2001-12-28')
        print('download done')
        #dailyreturn['ticker']=[x[0:6]for x in dailyreturn['ticker']]
        data_store=pd.HDFStore('DS.h5')                             #Create a storage object with fielname 'DRkey'
        data_store['DRkey']=dailyreturn                             #Put dailyreturn dataframe into tthe object setting the key as 'dailyreturn'
        data_store.close()
        return()
#Retrive the dailyreturn dataframe stored in the HDF file
    def Dailyreturn_retrieve(self):
        data_store=pd.HDFStore('DS.h5')                             #Access data　ｓｔｏｒｅ
        dailyreturn=data_store['DRkey']                             #Retrieve data using Key
        data_store.close()
        return(dailyreturn)
        
#Update the dailyreturn to the latest day and save it in the system as HDF file 增量
    def Dailyreturn_Update_Daily(self):
        dailyreturn=self.Dailyreturn_retrieve()
        maxday=dailyreturn['date'].max()
        newdaily=self.Returnhist(maxday)
        dailyreturn=dailyreturn.append(newdaily,ignore_index=True)
        #dailyreturn['ticker']=[x[0:6]for x in dailyreturn['ticker']]
        data_store=pd.HDFStore('DS.h5')                             #Create a storage object with fielname 'DRkey'
        data_store['DRkey']=dailyreturn                             #Put dailyreturn dataframe into tthe object setting the key as 'dailyreturn'
        data_store.close()
        print('Dailyreturn done')
        return()

    #Generate the bottom30% mcap bar of every trading day.
    def Lowmarketcap_bar(self,startdate,dailyreturn):
        rdailyreturn=dailyreturn.loc[dailyreturn['date']>startdate,:].copy()
        lowmcapbar={}
        daylist=list(rdailyreturn['date'].unique())
        for day in daylist:
            print(day)
            lowmcapbar[day]=np.percentile(rdailyreturn.loc[rdailyreturn['date']==day,'mcap'],30)
        lowmcapbardf=pd.DataFrame(lowmcapbar.items(),columns=['date','lowmcapbar'])
        return(lowmcapbardf)
    
    #Generate the bottom30% mcap bar of every rebal day.
    def Lowmarketcap_rebaldaylist(self,df,dailyreturn):
        daylist=list(df['date'].unique())
        rdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(daylist),:].copy()
        lowmcapbar={}
        for day in daylist:
            lowmcapbar[day]=np.percentile(rdailyreturn.loc[rdailyreturn['date']==day,'mcap'],30)
        lowmcapbardf=pd.DataFrame(lowmcapbar.items(),columns=['date','lowmcapbar'])
        return(lowmcapbardf)
    
    #Keep only the mcap in the top70%
    def Keep_only_70mcap(self,df,startdate,dailyreturn):
        lowmcapbardf=self.Lowmarketcap_bar(startdate,dailyreturn)
        rdailyreturn=dailyreturn.loc[dailyreturn['date']>startdate,:].copy()
        rdailyreturn['index']=rdailyreturn['ticker']+rdailyreturn['date']
        df['index']=df['ticker']+df['date']
        df=pd.merge(df,rdailyreturn[['index','mcap']],on='index',how='left')
        df=pd.merge(df,lowmcapbardf,on='date',how='left')
        df=df.loc[df['mcap']>=df['lowmcapbar'],:]
        df.drop(['mcap','lowmcapbar'],axis=1,inplace=True)
        return(df)
    
    #Input: a df with date, ticker and mcap, keep the stocks>70mcap of every rebalday
    def Keep_only_70mcapNew(self,df):
        df['bar']=df.groupby('date')['mcap'].transform(lambda x: x.quantile(0.3))
        df=df.loc[df['mcap']>df['bar'],:].copy()
        df=df.drop(['bar'],axis=1)
        return(df)
        
    #Each Signal's median 
    def Sector_valueratio_median_hist(self,df,signame):
        df['index']=df['date']+df['primecode']
        sector_valueratio_median=pd.DataFrame(df.groupby(['index'])[signame].median())
        sector_valueratio_median.reset_index(inplace=True)
        sector_valueratio_median['date']=sector_valueratio_median['index'].str[0:10]
        sector_valueratio_median['seccode']=sector_valueratio_median['index'].str[10:12]
        return(sector_valueratio_median)
    
    def Sector_valuaratio_maxmin(self,dfhist,signame,dfdaily):
        secmax=pd.DataFrame(dfhist.groupby(['seccode'])[signame].max())
        secmin=pd.DataFrame(dfhist.groupby(['seccode'])[signame].min())
        secmax.reset_index(inplace=True)
        secmin.reset_index(inplace=True)
        secmax.columns=['seccode',signame+'max']
        secmin.columns=['seccode',signame+'min']
        dfdaily=pd.merge(dfdaily,secmax,on='seccode',how='left')
        dfdaily=pd.merge(dfdaily,secmin,on='seccode',how='left')
        dfdaily[signame+'-%']=(dfdaily[signame]-dfdaily[signame+'min'])/(dfdaily[signame+'max']-dfdaily[signame+'min'])
        dfdaily=dfdaily.drop(columns=[signame+'max',signame+'min'])
        return(dfdaily)
    
    #Download the historical valuation metrics of stocks in df, produce the 1y-tile of the stocks
    def Stock_valuation_hist(self,df):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        tickerlist=list(df['ticker'].unique())
        query="Select convert(varchar,TradingDay, 23), SM.SecuCode,V.PE,V.PB,V.PS from JYDBBAK.dbo.LC_DIndicesForValuation V left join JYDBBAK.dbo.SecuMain SM  on V.InnerCode=SM.InnerCode where SM.SecuCode in  ("+str(tickerlist)[1:-1]+")  and V.TradingDay>'2005-01-01'"
        reslist=ms.ExecQuery(query)
        valuehist=pd.DataFrame(reslist,columns=['date','ticker','PE','PB','PS'])
        valuehist=valuehist.sort_values(by=['date','ticker'],ascending=[True,True])
        valuehist[['PB','PS','PE']]=valuehist[['PB','PS','PE']].astype(float)
        valuehist[['PB','PS','PE']]=valuehist[['PB','PS','PE']].fillna(method='ffill')
        for signame in ['PB','PS','PE']:
            valuehist[signame+'rollMax']=valuehist.groupby('ticker')[signame].apply(lambda g: g.rolling(250).max())
            valuehist[signame+'rollMin']=valuehist.groupby('ticker')[signame].apply(lambda g: g.rolling(250).min())
            valuehist[signame+'1y-tile']=(valuehist[signame]-valuehist[signame+'rollMin'])/(valuehist[signame+'rollMax']-valuehist[signame+'rollMin'])
            valuehist=valuehist.drop([signame+'rollMax',signame+'rollMin'],axis=1)
            valuehist.loc[valuehist[signame+'1y-tile'].isnull()==True,signame+'1y-tile']=valuehist.groupby('date')[signame+'1y-tile'].transform('mean')
        return(valuehist)
        
    def Bank_PB(self,df):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        tickerlist=list(df['ticker'].unique())
        rebaldaylist=list(df['date'].unique())
        query="Select convert(varchar,TradingDay, 23), SM.SecuCode,V.PB from JYDBBAK.dbo.LC_DIndicesForValuation V left join JYDBBAK.dbo.SecuMain SM  on V.InnerCode=SM.InnerCode where SM.SecuCode in  ("+str(tickerlist)[1:-1]+")  and V.TradingDay in ("+str(rebaldaylist)[1:-1]+")"
        reslist=ms.ExecQuery(query)
        PBhist=pd.DataFrame(reslist,columns=['date','ticker','PB'])
        return(PBhist)
        
    #Download and Calculate the median valuation (PE,PB,PS)of each sector 
    def Sector_valuation_median_hist(self,startdate,dailyreturn):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query="Select TradingDay, SM.SecuCode, V.PE,V.PB,V.PS from JYDBBAK.dbo.LC_DIndicesForValuation V left join JYDBBAK.dbo.SecuMain SM on V.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'"+startdate+"'"
        reslist=ms.ExecQuery(query)
        df=pd.DataFrame(reslist,columns=['date','ticker','PE','PB','PS'])
        df[['PE','PB','PS']]=df[['PE','PB','PS']].astype(float)
        df['date']=[str(x)[0:10]for x in df['date']]
        df=self.Keep_only_70mcap(df,startdate,dailyreturn)                                         #Only stock of top70%-tile will be taken into account
        sectormap=self.Stock_sector(list(df['date'].unique()),list(df['ticker'].unique()),'CITIC') #Historically, each stock's sector
        sectormap['primecode']=[str(x) for x in sectormap['primecode']]
        df['index']=df['ticker']+df['date']
        sectormap['index']=sectormap['ticker']+sectormap['date']
        df=pd.merge(df,sectormap[['index','primecode']],on='index',how='left')
        sector_PE_median_hist=self.Sector_valueratio_median_hist(df,'PE')
        sector_PB_median_hist=self.Sector_valueratio_median_hist(df,'PB')
        sector_PS_median_hist=self.Sector_valueratio_median_hist(df,'PS')
        sector_value_median_hist=pd.merge(sector_PE_median_hist,sector_PB_median_hist[['index','PB']],on='index',how='left')
        sector_value_median_hist=pd.merge(sector_value_median_hist,sector_PS_median_hist[['index','PS']],on='index',how='left')
        sector_value_median_hist=sector_value_median_hist[['date','seccode','PE','PB','PS']]
        sector_value_median_hist.to_csv("D:/SecR/sector_value_median_hist.csv",index=False)
        return(sector_value_median_hist)

    def Daily_sector_valuation_median_update(self,dailyreturn):
        SVMH=pd.read_csv("D:/SecR/sector_value_median_hist.csv")
        SVMH['seccode']=[str(x)for x in SVMH['seccode']]
        maxday=SVMH['date'].max()
        dailySVMH=self.Sector_valuation_median_hist(maxday,dailyreturn)
        SVMH=SVMH.append(dailySVMH)
        SVMH.to_csv("D:/SecR/sector_value_median_hist.csv",index=False)
        dfdaily=SVMH.loc[SVMH['date']==SVMH['date'].max(),:].copy()
        ayearago=str(int(maxday[0:4])-1)+maxday[4:]
        SVMH1y=SVMH.loc[SVMH['date']>=ayearago,:]
        for signame in ['PE','PB','PS']:
            dfdaily=self.Sector_valuaratio_maxmin(SVMH1y,signame,dfdaily)
        secname=self.Sec_name('CITIC')
        secname.columns=['seccode','sectorname']
        dfdaily=pd.merge(dfdaily,secname,on='seccode',how='left')
        dfdaily=dfdaily.loc[dfdaily['date']==dfdaily['date'].min(),:]
        dfdaily=dfdaily[['date','seccode','sectorname','PE','PB','PS','PE-%','PB-%','PS-%']]
        return(dfdaily)
    
    #update the tradingday file to get tradingday history of CSI from 2003-01-01
    def Tradingday(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="select distinct TradingDay from QT_Performance where TradingDay>'2003-01-01' order by TradingDay ASC"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['date'])
        df.to_csv("D:/SecR/Tradingday.csv",index=False)
        return()
    
    #Return the start and end date of backtest period
    def BTdays(self,rebalday,rebal_period):
        dateloc=tradingday.loc[tradingday['date']==rebalday,:].index[0]
        endloc=tradingday.shape[0]
        rebalstart=tradingday.iloc[dateloc+2,0]
        if dateloc+rebal_period+1<=endloc:
            rebalend=tradingday.iloc[dateloc+1+rebal_period,0]
        else:
            rebalend=tradingday['date'].max()
        return(rebalstart,rebalend)
        
    #Return the cumulative return of stocks in backtest period
    def Period_PNL(self,dailyreturn,sig_rebalday,rebalstart,rebalend):
        periodpnltab=dailyreturn.loc[(dailyreturn['ticker'].isin(sig_rebalday['ticker']))&(dailyreturn['date']>=rebalstart)&(dailyreturn['date']<=rebalend),:]
        periodpnlsum=periodpnltab.groupby(['ticker']).sum()
        periodpnlsum.reset_index(inplace=True)
        periodpnlsum['pnlrank']=periodpnlsum['dailyreturn'].rank(ascending=True)
        return(periodpnlsum)
        
       #Calculate the n rollingday cumulative return of Factors
    #Facreturn generated by def Facreturn
    def FacRollingReturn(self,rolldays,publisher):
        Facreturn=pd.read_csv("D:/SecR/FacReturn_"+publisher+"_SIZE.csv")
        FacLogreturn=Facreturn.drop('date',1)
        #Facdailyreturn=FacLogreturn.copy()
        if rolldays=='ITD':
            FacLogreturn=np.exp(np.log1p(FacLogreturn).cumsum())
        else:
            FacLogreturn=np.exp(np.log1p(FacLogreturn).rolling(rolldays).sum())
            #FacStdev=Facdailyreturn.rolling(rolldays).std()
            #FacSharpe=FacLogreturn/FacStdev
        #FacSharpe,FacLogreturn,FacStdev=map(lambda df: df.insert(loc=0, column='date',value=Facreturn['date']),[FacSharpe,FacLogreturn,FacStdev])
        #FacLogreturn.to_csv("D:/SecR/FacLogReturn_"+str(rolldays)+"day_CITIC_Size.csv",index=False)
        FacLogreturn.insert(loc=0, column='date',value=Facreturn['date'])
        return(FacLogreturn)
    
    #Get every stocks' sector return
    def StockSectorCumreturn(self,rebaldaylist,rolldays):
        faclogreturn=self.FacRollingReturn(rolldays,'CITIC')
        primecodelist=[s for s in faclogreturn.columns if s.isdigit()]
        sectorreturn=pd.melt(faclogreturn,id_vars=['date'],value_vars=primecodelist,var_name='primecode',value_name='secrollreturn')
        sectorreturn['index']=sectorreturn['date']+sectorreturn['primecode']
        stock_sector=self.Ashs_stock_seccode(rebaldaylist,primecodelist,'CITIC')
        stock_sector.loc[stock_sector['date']>=sectorreturn['date'].max(),'date']=sectorreturn['date'].max() 
        stock_sector['index']=stock_sector['date']+stock_sector['primecode']
        stock_sector=pd.merge(stock_sector,sectorreturn[['index','secrollreturn']],on='index',how='left')
        stock_sector=stock_sector.dropna()
        stock_sector=stock_sector.drop(['index'],1)
        return(stock_sector)
    
    #LongandShortterm(use 180+10days cumulative return as signal)
    def LStermSec(self,rebaldaylist):
        sigtab=self.StockSectorCumreturn(rebaldaylist,180)
        sigtab2=self.StockSectorCumreturn(rebaldaylist,10)
        sigtab['index']=sigtab['date']+sigtab['ticker']
        sigtab2['index']=sigtab['date']+sigtab['ticker']
        sigtab=pd.merge(sigtab,sigtab2[['index','secrollreturn']],on='index',how='left')
        sigtab['combreturn']=sigtab['secrollreturn_x']+sigtab['secrollreturn_y']
        sigtab=sigtab[['date','ticker','primecode','combreturn']]
        return(sigtab)
    
    #Use Long/Short momentum+1month reversal to decide the ranking of sectors
    def SecSignal(self,publisher):
        faclogreturn=self.FacRollingReturn(180,publisher)
        #faclogreturn2=self.FacRollingReturn(30,publisher)
        faclogreturn3=self.FacRollingReturn(10,publisher)
        faclogreturnG=faclogreturn.iloc[:,1:]+faclogreturn3.iloc[:,1:]
        #faclogreturnG=faclogreturn.iloc[:,1:]+faclogreturn2.iloc[:,1:]
        #faclogreturnG=faclogreturnG-faclogreturn3.iloc[:,1:]
        primecodelist=[s for s in faclogreturnG.columns if s.isdigit()]
        faclogreturnG=faclogreturnG[primecodelist].copy()
        faclogreturnG['date']=faclogreturn['date']
        return(faclogreturnG)
    
    #Produce first year, secondyear, third year, synthesized years consensus metric sigdatatab
    #format of signal='0_con_or_yoy' or signal='1_con_or_yoy'
    def ConsensusExtract(self,signal,rebaldaylist,dailyreturn):
        signame=signal
        loc=tradingday.loc[tradingday['date']==rebaldaylist[0],:].index[0]
        firstday=tradingday.iloc[loc-20,0]
        newrebaldaylist=rebaldaylist.copy()
        newrebaldaylist.insert(0,firstday)
        newdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(newrebaldaylist),:].copy()
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="zyyx")
        query="select convert(varchar,con_date,23),convert(varchar,con_date,23),stock_code,con_year, "+signame+", con_np_type from zyyx.dbo.con_forecast_stk where con_date in  ("+str(newrebaldaylist)[1:-1]+")"
        reslist=ms.ExecQuery(query)
        df=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','con_year',signame,'con_np_type'])
        #year_ahead=int(signal[0:1])
        df=df.loc[df['con_np_type']==1,:].copy()
        df['targetyear']=df['publdate'].str[0:4]
        df['targetyear']=df['targetyear'].astype(int)+1
        selectdf=df.loc[df['con_year']==df['targetyear'],:].copy()
        selectdf=selectdf.loc[selectdf[signame].isnull()==False,:]
        selectdf=selectdf.loc[selectdf[signame]!=0,:]
        selectdf[signame]=1/selectdf[signame]
        #selectdf['index']=selectdf['publdate']+selectdf['ticker']
        #newdailyreturn['index']=newdailyreturn['date']+newdailyreturn['ticker']
        #selectdf=pd.merge(selectdf,newdailyreturn[['index','closeprice']],on='index',how='left')
        #selectdf=selectdf.loc[selectdf['closeprice'].isnull()==False,:]
        #selectdf[signame]=selectdf[signame]/selectdf['closeprice']
        #selectdf=selectdf.sort_values(by=['ticker','publdate'],ascending=[True,True])
        #selectdf['diff']=selectdf.groupby(['ticker'])[signame].diff()
        #selectdf['shift']=selectdf.groupby(['ticker'])[signame].shift()
        #selectdf=selectdf.loc[selectdf['shift'].isnull()==False,:].copy()
        #selectdf[signame]=selectdf['diff']/selectdf['shift']
        selectdf=selectdf[['publdate','enddate','ticker',signame]]
        selectdf.columns=['publdate','enddate','ticker','sigvalue']
        selectdf=selectdf.dropna()
        return(selectdf)

class WeightScheme():
    def __init__(self):
        self.DC=DataCollect()
        self.DS=DataStructuring()
    
    #Calculate total sector weight of each sector 
    def BM_sectorweight(self,startdate,benchmark,publisher):
        membhist=self.DC.Benchmark_membs(benchmark,startdate)
        tickerlist=list(membhist['ticker'].unique())
        daylist=list(membhist['date'].unique())
        stock_sector=self.DC.Stock_sector(daylist,tickerlist,publisher)
        membhist['index']=membhist['date']+membhist['ticker']
        stock_sector['index']=stock_sector['date']+stock_sector['ticker']
        membhist=pd.merge(membhist,stock_sector[['index','primecode']],on='index',how='left')
        membhist['index']=membhist['date']+membhist['primecode']
        bm_sectorweight=pd.DataFrame(membhist.groupby(['index'])['weight'].sum())
        bm_sectorweight.reset_index(inplace=True)
        bm_sectorweight.columns=['index','secweight']
        bm_sectorweight['date']=bm_sectorweight['index'].str[0:10]
        bm_sectorweight['primecode']=bm_sectorweight['index'].str[10:]
        bm_sectorweight=bm_sectorweight[['date','primecode','secweight']]
        return(bm_sectorweight)
    
    #input: df with date, ticker (or %portNAV)
    #output: add a column of mixed primecode  for 非银:breakdown to 保险、券商&综合金融
    def Stock_sectorMixed(self,df):
        if 'primecode' in list(df.columns):
            df=df.drop(['primecode'],axis=1)
        df=self.DC.Sector_get(df,'CSI')
        df=df.rename(columns={'primecode':'CSIprimecode'})
        df=self.DC.Sector_get(df,'CITIC')
        df=df.rename(columns={'primecode':'CITICprimecode'})
        df=self.DC.Second_sector_get(df)
        df.loc[df['CITICprimecode']=='41','CITICprimecode']=df.loc[df['CITICprimecode']=='41','secondcode']
        df.loc[(df['CSIprimecode']=='06')&(~df['CITICprimecode'].str[0:2].isin(['40','41','42'])),'CITICprimecode']='4130'
        df.loc[df['CSIprimecode']=='06','CSIprimecode']=df.loc[df['CSIprimecode']=='06','CITICprimecode']
        df=df.rename(columns={'CSIprimecode':'primecode'})
        df=df.drop(['CITICprimecode','secondcode'],axis=1)
        return(df)
    
    
    def BM_sectorweight_mixed(self,benchmark,startdate):
        membhist=self.DC.Benchmark_membs(benchmark,startdate)
        stock_sectormixed=self.Stock_sectorMixed(membhist)
        stock_sectormixed['index']=stock_sectormixed['date']+stock_sectormixed['primecode']
        bm_sectorweight=pd.DataFrame(stock_sectormixed.groupby('index')['weight'].sum())
        bm_sectorweight.reset_index(inplace=True)
        bm_sectorweight.columns=['index','secweight']
        bm_sectorweight['date']=bm_sectorweight['index'].str[0:10]
        bm_sectorweight['primecode']=bm_sectorweight['index'].str[10:]
        bm_sectorweight=bm_sectorweight[['date','primecode','secweight']]
        return(bm_sectorweight)
        
    
    #intersect the selected stocks with a benchmark    
    def Benchmark_intersect(self,df,benchmark):
        startdate=df['date'].min()
        rebaldaylist=df['date'].unique()
        membhist=self.DC.Benchmark_membs(benchmark,startdate)
        rebaldaydf=pd.DataFrame(rebaldaylist,columns=['date'])
        rebaldaydf['indexrebalday']=np.nan
        indexrebaldaydf=pd.DataFrame(membhist['date'].unique(),columns=['date'])
        indexrebaldaydf['indexrebalday']=indexrebaldaydf['date']
        rebaldaydf=rebaldaydf.append(indexrebaldaydf)
        rebaldaydf=rebaldaydf.sort_values(['date'],ascending=[False])
        rebaldaydf['indexrebalday']=rebaldaydf['indexrebalday'].fillna(method='bfill')
        rebaldaydf=rebaldaydf.sort_values(by=['date','indexrebalday'],ascending=[True,True])
        rebaldaydf=rebaldaydf.drop_duplicates(['date'],keep="first")
        newdf=pd.merge(df,rebaldaydf,on='date',how='left')
        newdf['index']=newdf['ticker']+newdf['indexrebalday']
        membhist['index']=membhist['ticker']+membhist['date']
        newdf=pd.merge(newdf,membhist[['index','weight']],how='left',on='index')
        newdf=newdf.loc[newdf['weight']>0,:]
        newdf=newdf.drop(columns=['indexrebalday','index'])
        return(newdf)
    
    #Get the CITICS second industry of stocks and recategorize them into 40：银； 41：证； 42：地产； 4120：保险
    def FIG_Recategorize(self,df):
        df=self.DC.Second_sector_get(df)
        df.loc[df['secondcode'].isin(['4020','4010','4030']),'secondcode']='40'          #All banks are 40
        df.loc[df['secondcode'].isin(['4210','4220']),'secondcode']='42'      #All Realestates are 42
        df.loc[df['secondcode'].isin(['4110','4310','4320','4330','4130']),'secondcode']='41' #All brokers and diversified finance are 41
        df.loc[df['secondcode'].isin(['4120']),'secondcode']='4120'               #All 4120 are Insurance
        return(df)
    
    #Given a postab, return the stocks' correspondent sectors' weight in a benchmark 
    #Special Treatment about Financial stocks: 
    def Benchmark_sector_weight(self,df,benchmark):
        startdate=df['date'].min()
        rebaldaylist=list(df['date'].unique())
        df=self.FIG_Recategorize(df)
        membhist=self.Benchmark_itself_sector_weight(benchmark,startdate)
        bm_sector_rebalday=self.DS.Rebalday_alignment(membhist,rebaldaylist)
        bm_sector_rebalday=bm_sector_rebalday.drop_duplicates()
        bm_sector_rebalday['index']=bm_sector_rebalday['date']+bm_sector_rebalday['secondcode']
        df['index']=df['date']+df['secondcode']
        df=pd.merge(df,bm_sector_rebalday[['index','sector%']],on='index',how='left')
        return(df,bm_sector_rebalday)
    
    #Every month end's sector % with FIG readjusted
    def Benchmark_itself_sector_weight(self,benchmark,startdate):
        membhist=self.DC.Benchmark_membs(benchmark,startdate)
        membhist=self.FIG_Recategorize(membhist)
        membhist['index']=membhist['date']+membhist['secondcode']
        membhist['sector%']=membhist.groupby(['index'])['weight'].transform('sum')
        membhist=membhist[['date','secondcode','sector%']]
        membhist['sector%']=membhist['sector%']/100
        return(membhist)
        
        
   #allocate weights to sector according to CSI sector weights
    def MixedSecWeight(self,df):
        df=self.Stock_sectorMixed(df)
        bms=self.BM_sectorweight_mixed('CSI300','2005-11-28')
        daylistmerge=self.DS.Daylistmerge(df,bms)
        bms['secweight']=bms['secweight']/100
        df=pd.merge(df,daylistmerge,on='date',how='left')
        df['index']=df['oldday']+df['primecode']
        bms['index']=bms['date']+bms['primecode']
        df=pd.merge(df,bms[['index','secweight']],on='index',how='left')
        df=df.drop(columns=['oldday','index'])
        return(df)
    
    #allocate weights to sector according to CSI sector weights
    def SecWeight(self,df):
        if 'primecode' not in list(df.columns):
            df=self.DC.Sector_get(df,'CSI')
        bms=self.BM_sectorweight('2005-11-28','CSI300','CSI') #Download the historical holding, dates=month end
        daylistmerge=self.DS.Daylistmerge(df,bms)
        bms['secweight']=bms['secweight']/100
        df=pd.merge(df,daylistmerge,on='date',how='left')
        df['index']=df['oldday']+df['primecode']
        bms['index']=bms['date']+bms['primecode']
        df=pd.merge(df,bms[['index','secweight']],on='index',how='left')
        df=df.drop(columns=['oldday','index'])
        return(df)
    
    #Given mirrored weight from the benchmark, calculate PortNAV%
    def Generate_PortNav(self,df):
        totalweight=df.groupby(['date'],as_index=False).agg({"weight":"sum"})
        totalweight=totalweight.rename(columns={'weight':'totalw'})
        df2=pd.merge(df,totalweight,on='date',how='left')
        df2['PortNav%']=df2['weight']/df2['totalw']
        df2['PortNav%']=df2['PortNav%'].astype(float)
        df2['PortNav%']=df2['PortNav%'].round(4)
        df2=df2[['date','ticker','PortNav%']]
        return(df2)
        
    #Given mirrored weight from the benchmark, calculate PortNAV%
    def Generate_PortNavEqual(self,df):
        df['weight']=1
        totalweight=df.groupby(['date'],as_index=False).agg({"weight":"sum"})
        totalweight=totalweight.rename(columns={'weight':'totalw'})
        df2=pd.merge(df,totalweight,on='date',how='left')
        df2['PortNav%']=df2['weight']/df2['totalw']
        df2['PortNav%']=df2['PortNav%'].astype(float)
        df2['PortNav%']=df2['PortNav%'].round(4)
        df2=df2[['date','ticker','PortNav%']]
        return(df2)
        
    def Generate_PortNavMcap(self,df,dailyreturn):
        df['index']=df['date']+df['ticker']
        rebald=list(df['date'].unique())
        dailymcap=dailyreturn.loc[dailyreturn['date'].isin(rebald)].copy()
        dailymcap['index']=dailymcap['date']+dailymcap['ticker']
        df=pd.merge(df,dailymcap[['index','mcap']],on='index',how='left')
        totalweight=df.groupby(['date'],as_index=False).agg({"mcap":"sum"})
        totalweight=totalweight.rename(columns={'mcap':'totalmcap'})
        df2=pd.merge(df,totalweight,on='date',how='left')
        df2['PortNav%']=df2['mcap']/df2['totalmcap']
        df2['PortNav%']=df2['PortNav%'].astype(float)
        df2['PortNav%']=df2['PortNav%'].round(4)
        df2=df2[['date','ticker','PortNav%']]
        return(df2)
    
    #Cross sectors PortNav%, return the %weight of stocks in each sector,total mcap is the total of the date of the sector
    def Generate_PortNavMcap_allsecs(self,df,dailyreturn):
        df['index']=df['date']+df['ticker']
        rebald=list(df['date'].unique())
        dailymcap=dailyreturn.loc[dailyreturn['date'].isin(rebald)].copy()
        dailymcap['index']=dailymcap['date']+dailymcap['ticker']
        df=pd.merge(df,dailymcap[['index','mcap']],on='index',how='left')
        df['index']=df['date']+df['primecode']
        totalweight=df.groupby(['index'],as_index=False).agg({"mcap":"sum"})
        totalweight=totalweight.rename(columns={'mcap':'totalmcap'})
        df2=pd.merge(df,totalweight,on='index',how='left')
        df2['PortNav%']=df2['mcap']/df2['totalmcap']
        df2['PortNav%']=df2['PortNav%'].astype(float)
        df2['PortNav%']=df2['PortNav%'].round(4)
        df2=df2[['date','ticker','primecode','PortNav%']]
        return(df2)
    
    
    def Gnerate_PortNavMcapTier(self,df,dailyreturn):
        df['index']=df['date']+df['ticker']
        rebald=list(df['date'].unique())
        dailymcap=dailyreturn.loc[dailyreturn['date'].isin(rebald)].copy()
        dailymcap['index']=dailymcap['date']+dailymcap['ticker']
        df2=pd.merge(df,dailymcap[['index','mcap']],on='index',how='left')
        df2['rank']=df2.groupby('date')['mcap'].rank(ascending=False)
        df2['datecount']=df2.groupby('date')['date'].transform('count')
        df2['date_count']=df2['datecount']/3
        df2['date_count']=[int(x) for x in df2['date_count']]
        df2['PortNav%']=0
        df2.loc[df2['rank']<=df2['date_count'],'PortNav%']=0.6/df2['date_count']
        df2.loc[(df2['rank']>df2['date_count'])&(df2['rank']<=df2['date_count']*2),'PortNav%']=0.3/df2['date_count']
        df2.loc[(df2['rank']>df2['date_count']*2),'PortNav%']=0.1/(df2['datecount']-2*df2['date_count'])
        df2=df2[['date','ticker','PortNav%']]
        return(df2)
    
    #After generating list of stocks candidates per rebalday, screenout the nonactive stocks
    def Active_stock_screening(self,df,dailyreturn,rebaldaylist):
        activestock=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist))&(dailyreturn['dailyreturn']!=0),:].copy()
        activestock['index']=activestock['date']+activestock['ticker']
        df['index']=df['date']+df['ticker']
        df=df[df['index'].isin(activestock['index'])]
        df=df.drop('index', 1)
        return(df)

class DataStructuring():
    def __init__(self):
        self.DC=DataCollect()
    
    def Addindex(self,df):
        df['index']=df['date']+df['ticker']
        return(df)
        
    def Screen_Ashs(self,df):
        df=df.loc[df['ticker'].str.len()==6,:]   #No HK shs
        df=df.loc[df['ticker'].str[0].isin(['6','0','3'])]
        return(df)
        
    #merge df1 main dataframe with df2 with one columnn added, both need to have dates and tickers as index
    def Data_merge(self,df1,df2,newcolname):
        df1['index']=df1['date']+df1['ticker']
        df2['index']=df2['date']+df2['ticker']
        df1=pd.merge(df1,df2[['index',newcolname]],on='index',how='left')
        df1=df1.drop(columns=['index'])
        return(df1)
        
         #*****Verified****
    def Dfquantile(self,df):
        mask=np.isnan(df)
        quintiles=np.nanpercentile(df,[20,40,60,80],axis=1).transpose() #The higher the cumAlpha, the higher the Group number
        gpingnp=[np.vstack(tuple(np.searchsorted(quintiles[i],df.iloc[i,:])for i in range(0,df.shape[0])))]
        df_quintiles=pd.DataFrame(gpingnp[0])
        df_quintiles.columns=df.columns
        df_quintiles.index=df.index
        df_quintiles[mask]=np.nan
        df_quintiles=df_quintiles+1 #the grouping would be 0-4 if not +1
        return(df_quintiles)
        
    #input the dataframe of quintiles, calculate mean since day1
    #The problem of this is, 1, current stock with NAN will carry old mean 2. the stock with only very short trakcrecord will have mean q too
    def Dfmean(self,df):
        df2=df.copy()                   #Df is the dataframe that carries Q of cumulative returns, this is ONE-to-ONE to the original cumulative return table
        mask=np.isnan(df2)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            meanmatrix=[np.vstack(tuple(np.nanmean(df2.iloc[0:i,:],axis=0)for i in range(1,(df2.shape[0]+1))))] #calculate the ITD mean of Q
        dfmean=pd.DataFrame(meanmatrix[0])
        dfmean.columns=df.columns
        dfmean.index=df2.index
        dfmean[mask]=np.nan                           #This step makes the mean won't carried to the supposedly nan days
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            meanmatrix=[np.vstack(tuple(np.average((df2.iloc[(i-60):i,:]),axis=0))for i in range(1,(df2.shape[0]+1)))] #the last 60day average of Q, keep nan.
        df60dmean=pd.DataFrame(meanmatrix[0])
        df60dmean.columns=df.columns
        df60dmean.index=df2.index
        mask=np.isnan(df60dmean)         #If the last 60days have nan, then that day's Qmean will be NAN. So only days with 60day consecutive non-nan data will be taken into account
        dfmean[mask]=np.nan
        return(dfmean)
        
    #Axis1 rank for dataframe
    def Dfrank(self,df):
        df2=df.copy()
        mask=np.isnan(df)
        df2[mask]=0                    #make the nonactive analysts' average qrating as 0, always be the lowest ranked
        df2=df2.rank(axis=1,ascending=True,method='average')
        return(df2)
    
    #Winsorize a given dataframe
    def Winsorize(self,df,colname,tile):
        dfpivot2=df[colname].astype(float).values
        mask=np.isnan(dfpivot2)
        wnp=mstats.winsorize(dfpivot2,limits=[tile,tile],axis=0,inplace=True)
        wnp[mask]=np.nan                                                      #Inplace true will fill all np.nan value with extreme value
        df[colname]=wnp
        return (df)
    
    #use SKlearn instead of Statsmodels to estimate residuals
    def Neutralization(self,df,sig,Xset):
        reg=LinearRegression().fit(df.loc[:,Xset], df['sigvalue'])
        est=reg.predict(df.loc[:,Xset])
        residuals=df['sigvalue']-est
        csg=df[['ticker','sigvalue']].copy()
        csg['N_'+sig]=residuals
        return(csg)
        
    #Given the table that include columns (Independant Vairables:X and Dependant Variables:Y)
    def Neutralization_SM(self,df,sig,Xset):
        est=sm.OLS(df['sigvalue'],df.loc[:,Xset]).fit()
        csg=df[['ticker','sigvalue']].copy()
        csg['N_'+sig]=est.resid.values
        return(csg)
    
    #input: a dataframe and a list of column names, return the grouping of these columns values as new columns with names starting with "Q_"
    def Qgrouping(self,signame,df,ngroup):
        qlabels=list(range(1,ngroup+1))
        qgroup=pd.qcut(df[signame],len(qlabels),labels=qlabels)  #return the grouping of the signame column of a dataframe
        df['Q']=qgroup
        return(df)
    
    #Save the each q's portfolio in a dictionary with index=rebaldate+signalname+qtier
    def Qport(self,df,colname,rebalday,portdict):
        qlist=df[colname].unique()
        for q in qlist:
            newqport=df.loc[df[colname]==q,['ticker','mcap',colname]]
            newqport['date']=rebalday
            portindex=rebalday+'_'+colname+'_'+str(q)
            portdict[portindex]=newqport
        return(portdict)
        
    def Qport2(self,df,colname,rebalday,portdict):
        qlist=df[colname].unique()
        for q in qlist:
            newqport=df.loc[df[colname]==q,['ticker','mcap',colname]]
            newqport['date']=rebalday
            newqport['PortNav%']=1/newqport.shape[0]
            newqportlist=newqport.values.tolist()
            portindex=colname+'_'+str(q)
            if not portindex in portdict:
                portdict[portindex]=[]
            portdict[portindex].extend(newqportlist)
        return(portdict)
    
    
    #turn a dicitonary of facname1_date1, facname2_date2 into facname1_q1, facname2_q2....
    def Facqport(self,olddict,facname,rebaldaylist,portdict):
        firstdf=olddict[list(olddict.keys())[0]]
        if 'Q' in firstdf.columns:
            qlist=list(firstdf['Q'].unique())
            for rebalday in rebaldaylist:
                for q in qlist:
                    newqport=olddict[facname+'_'+rebalday].loc[olddict[facname+'_'+rebalday]['Q']==q,['ticker']]
                    newqport=newqport.drop_duplicates()
                    newqport['date']=rebalday
                    newqport['PrtNav%']=1/newqport.shape[0]
                    newqportlist=newqport.values.tolist()
                    portindex=facname+'_'+str(q)
                    if not portindex in portdict:
                        portdict[portindex]=[]
                    portdict[portindex].extend(newqportlist)
        else:
            for rebalday in rebaldaylist:
                newqport=olddict[facname+'_'+rebalday].loc[:,['ticker']]
                newqport=newqport.drop_duplicates()
                newqport['date']=rebalday
                newqport['PrtNav%']=1/newqport.shape[0]
                newqportlist=newqport.values.tolist()
                portindex=facname+'_5'
                if not portindex in portdict:
                        portdict[portindex]=[]
                portdict[portindex].extend(newqportlist)
        return(portdict)
    
    #every factor has one dataframe of every rebalday's positions lined up
    def Facport(self,olddict,facname,rebaldaylist,portdict):
        facport=olddict[facname+'_'+rebaldaylist[0]].copy()
        facport['date']=rebaldaylist[0]
        for rebalday in rebaldaylist[1:]:
            temport=olddict[facname+'_'+rebalday].copy()
            temport['date']=rebalday
            facport=facport.append(temport)
        portdict[facname]=facport
        return(portdict)

    #Calculate the growth or vol of a metric (YOY)
    def GrowVol(self,sighist,growvol):
        sgv=sighist.copy()
        sgv=sgv.loc[(sgv['sigvalue']!=0)|(sgv['sigvalue'].isnull==False),:]
        sgv['sigvalue']=sgv['sigvalue'].astype(float)
        sgv['enddate']=sgv['enddate'].astype(str)
        sgv['month']=sgv['enddate'].str[5:7].astype(int)
        sgv['year']=sgv['enddate'].str[0:4].astype(int)
        sgv=sgv.sort_values(by=['ticker','signame','month','enddate'],ascending=[True,True,True,True])
        sgv['yeardiff']=sgv['year']-sgv['year'].shift(1)
        sgv['index']=sgv['ticker']+sgv['signame']+sgv['month'].astype(str)
        sgv['nthoccur']=sgv.groupby('index').cumcount()+1                                         #return the nth occurence of the index
        if growvol=='grow':
            sgv['deriv']=(sgv['sigvalue']-sgv['sigvalue'].shift(1))/abs(sgv['sigvalue'].shift(1)) #growth
            sgv.loc[(sgv['nthoccur']==1)|(sgv['yeardiff']!=1),'deriv']=np.nan                    #Get rid of the non last year data and first occurence of the growth (it might include data of diff category)
            sgv['signame']=sgv['signame']+'growth'
        elif growvol=='vol':
            sgv['deriv']=sgv['sigvalue'].rolling(3).std()                                         #volatility of the signale
            sgv.loc[(sgv['nthoccur']<3)|(sgv['yeardiff']!=1),'deriv']=np.nan                        #Get rid of the non last year data and top2 occurence of vol, (it might include data of diff category)
            sgv['signame']=sgv['signame']+'vol'
        sgv=sgv.loc[sgv['deriv'].isnull()==False,:]                                                       
        sgv['sigvalue']=sgv['deriv']
        sgv=sgv[['publdate','enddate','ticker','sigvalue','signame']]                             #combine it with the sighist
        sighist=sighist.append(sgv)
        return(sighist)
    
    #Input: portfolio to be neutralized and benchmark...to neutralize the portfolio's exposure to marketcap and industry exposure vs the benchmark
    def Optimize(self,port,bm,targetfactor,CSIcol):
        stock_num=port['weight'].shape[0]
        CSIdummymatrix=bm[CSIcol].values
        Cweight=bm['weight']
        Cindustryexp=np.dot(Cweight,CSIdummymatrix)
        Adummymatirx=port[CSIcol].values
        def statistics(weights):
            weights=np.array(weights)
            t=port[targetfactor]
            s=np.dot(weights.T,t)
            return s
        def fac_exposure_objective(weights):
            return-statistics(weights)
        cons=({'type':'eq','fun': lambda x: np.sum(x)-1},
               {'type':'eq','fun': lambda x:-np.linalg.norm(np.dot(x,Adummymatirx)-Cindustryexp)})
        bnds=tuple((0,1) for x in range(stock_num))    
        print('running optimization')
        res=minimize(fac_exposure_objective,[0]*stock_num,constraints=cons,bounds=bnds,method='SLSQP')
        return(res['x'])
    
    #return the othogonized matrix 
    def Othogonize(self,df):
        df=df.dropna()
        df=df.reset_index(drop=True)
        newdf=df.drop('ticker',1)
        colnames=newdf.columns
        arraytbeo=np.array(newdf)
        otho=pd.DataFrame(Opt.Gram_Schmidt(arraytbeo),columns=colnames)
        otho.insert(0,'ticker',df['ticker'])
        otho[colnames]=otho[colnames].apply(lambda x:stats.zscore(x))
        return(otho)
      
        #Given date and ticker, get mcap and dummy sector variables as columns on the right
    def Mcap_sector(self,stock_sector,dailyreturn,df):
        rebaldaylist=df['date'].unique()
        rebaldaylist.sort
        rdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),:].copy()
        df,rdailyreturn,stock_sector=map(self.Addindex,(df,rdailyreturn,stock_sector))
        df=pd.merge(df,rdailyreturn[['index','mcap']],on='index',how='left')
        df=pd.merge(df,stock_sector[['index','primecode']],on='index',how='left')
        indu_dummy=pd.get_dummies(df['primecode'])
        df=pd.concat([df,indu_dummy],axis=1)  
        return(df)  
    
        #use WLS for cross-sectional regression    
    def WLS(self,df,Y,Xset,Weightcol):
        Y=np.array(df[Y])
        X=np.array(df[Xset])
        Weight=np.array(df[Weightcol])
        wls_model = sm.WLS(Y,X, weights=Weight)
        results= wls_model.fit()
        coefficients=results.params
        return(coefficients)
    
    #Get the latest enddate record of a stock (single metric only)
    def Get_lastrecord(self,df):
        df=df.loc[df['ticker'].str[0].isin(['6','0','3'])].copy() 
        df=df.drop_duplicates(subset=['enddate','ticker'],keep='first')
        df=df.sort_values(by=['enddate'],ascending=True)
        df=df.drop_duplicates(subset=['ticker'],keep='last')
        df=df.drop(['publdate','enddate'],axis=1)
        df['sigvalue']=df['sigvalue'].astype(float)
        return(df)
    
    #tranlsate the another rebaldaylist to my rebaldaylist, taking the latest dates available
    def Daymerge(self,olddaylist,rebaldaylist):
        olddaydf=pd.DataFrame(olddaylist,columns=['oldday'])
        olddaydf['olddday2']=olddaydf['oldday']
        daylistmerge=rebaldaylist.copy()
        daylistmerge.extend(olddaydf['oldday'].unique())
        daylistmerge.sort()
        daylistmerge=pd.DataFrame(daylistmerge,columns=['oldday'])
        daylistmerge=pd.merge(daylistmerge,olddaydf,on='oldday',how='left')
        daylistmerge=daylistmerge.fillna(method='ffill')
        daylistmerge.columns=['date','oldday']
        daylistmerge=daylistmerge.drop_duplicates()
        daylistmerge=daylistmerge.loc[daylistmerge['date'].isin(rebaldaylist)]
        return(daylistmerge)
    
       #enter a df with rebalday, return the historical sector weights of an index:
    def Daylistmerge(self,df1,df2):
        daylist1=list(df1['date'].unique())
        daylist2=list(df2['date'].unique())
        daylistmerge=self.Daymerge(daylist2,daylist1)
        return(daylistmerge)
    
      #Combine Shen5 and Shen6 stocks, align the dates, df is any df with rebaldaylist as date column
      #df=pd.read_csv("D:/SecR/NewThreeFour_20200802.csv")
    def Shen56(self,df):
        df['ticker']=[str(x)for x in df['ticker']]
        df['ticker']=df['ticker'].str.zfill(6)
        shen6=pd.read_csv("D:/SecR/Shen6_stocks.csv")
        shen6=shen6.loc[shen6['ticker'].str.len()==9,:]   #No HK shs
        shen6=shen6.loc[shen6['ticker'].str[0].isin(['6','0','3'])]
        shen6['ticker']=[x[0:6] for x in shen6['ticker']]
        olddaylist=list(shen6['date'].unique())
        rebaldaylist=list(df['date'].unique())
        daylistmerge=self.Daymerge(olddaylist,rebaldaylist)
        daydict=dict(zip(daylistmerge['date'],daylistmerge['oldday']))
        daylist=list(df['date'].unique())
        shen56=pd.DataFrame(columns=['date','ticker'])
        for day in daylist:
            shen5temp=df.loc[df['date']==day,['date','ticker']]
            shen6day=daydict[day]
            shen6temp=shen6.loc[shen6['date']==shen6day,['date','ticker']]
            shen6temp['date']=day
            shen56=shen56.append(shen5temp)
            shen56=shen56.append(shen6temp)
        shen56=shen56.drop_duplicates()
        return(shen56)
    
    #for universe like Shen56 that are saved in harddrive, convert the dates to the rebalday (on notch later than the universe date) that would use the universe
    def Rebalday_alignment_old(self,universe,rebaldaylist):
        universe=universe.drop_duplicates()                              
        daylist=list(universe['date'].unique())                                         #This part, convert the dates of universe into the dates of rebaldaylist prior to the universe date, to make sure rebalday is using the latest available universe
        daylistmerge=self.Daymerge(daylist,rebaldaylist)
        daydict=dict(zip(daylistmerge['oldday'],daylistmerge['date']))
        universe=universe.loc[(universe['date']>=min(daydict.keys()))&(universe['date']<=max(daydict.keys())),:]
        universe['date']=[daydict[x]for x in universe['date']]
        return(universe)
    
    #This solves the problem of having more than one rebalday 
    #Given any df that needs to be followed (CSI300 rebal or Shen56) and rebaldaylist, produce the position table following the latest benchmark/shen56 rebalance day
    def Rebalday_alignment(self,df,rebaldaylist):
        daydf=pd.DataFrame(list(df['date'].unique()),columns=['date'])
        rebaldaydf=pd.DataFrame(rebaldaylist,columns=['date'])
        daydf['tocopy']=daydf['date']
        rebaldaydf['tocopy']=np.nan
        daydf=daydf.append(rebaldaydf)
        daydf=daydf.sort_values(by=['date'],ascending=True)
        daydf['tocopy']=daydf['tocopy'].fillna(method='ffill')
        tocopydict=dict(zip(daydf['date'],daydf['tocopy']))              #daydf's date is rebalday, tocopy is the bmrebalday
        dfdict={}
        for bmrebalday in list(df['date'].unique()):
            dfdict[bmrebalday]=df.loc[df['date']==bmrebalday,~df.columns.isin(['date'])].copy()
        newdict={}
        for keys in list(daydf['date'].unique()):
            tocopydate=tocopydict[keys]
            rebaldaydf=dfdict[tocopydate].copy()
            rebaldaydf['date']=keys
            newdict[keys]=rebaldaydf.copy()
        newdf=pd.DataFrame()
        for k,v in newdict.items():
            newdf=newdf.append(v)
        newdf.columns=newdict[list(newdict.keys())[0]].columns
        datecol=newdf.pop('date')
        newdf.insert(0,column='date',value=datecol)
        newdf=newdf.loc[newdf['date'].isin(rebaldaylist),:].copy()
        return(newdf)

class ReturnCal():
    def __init__(self):
        self.DC=DataCollect()
        self.DS=DataStructuring()
        self.WS=WeightScheme()
    
    #SummaryPNL=A.Backtest('2015-12-28',20)
    #This is the new version of PNL calculation, produce PNL daily in one go. Take historical position as input. Tradedate=Rebaldate+2    
    def DailyPNL(self,dailyreturn,postab):
        tradetab=postab.pivot_table(index='date',columns='ticker',values='PortNav%',aggfunc='first')
        tradetab=tradetab.fillna(0)
        tradetab.reset_index(level=0,inplace=True)
        tradedayseries=tradingday.loc[tradingday['date']>=postab['date'].min(),['date']]
        newtradetab=pd.merge(tradedayseries,tradetab,on='date',how='left')
        newtradetab=newtradetab.fillna(method='ffill')
        newtradetab=newtradetab.loc[newtradetab['date']<=dailyreturn['date'].max(),:]
        newtradetab['date']=newtradetab['date'].shift(-2)
        newtradetab=newtradetab.loc[newtradetab['date'].isnull()==False,:]
        newtradetab=newtradetab.fillna(0)
        returntab=dailyreturn.loc[(dailyreturn['date'].isin(newtradetab['date']))&(dailyreturn['ticker'].isin(newtradetab.columns)),:]
        returntab=returntab.pivot_table(index='date',columns='ticker',values='dailyreturn')
        returntab.reset_index(level=0,inplace=True)
        returntab=returntab[newtradetab.columns]
        returntab=returntab.fillna(0)
        SPNL=pd.DataFrame(returntab.iloc[:,1:].values*newtradetab.iloc[:,1:].values,columns=returntab.columns[1:],index=returntab.index)
        SPNL['dailyreturn']=SPNL.sum(axis=1)
        SPNL['date']=returntab['date'].copy()
        SPNL=SPNL[['date','dailyreturn']]
        return(SPNL)
        
    #Caclulate cuulative PNL
    def CumPNL(self,SPNL):
        SPNL=pd.merge(SPNL,tradingday,on='date',how='left')
        SPNL['StratCml']=np.exp(np.log1p(SPNL['dailyreturn']).cumsum())
        return(SPNL)
    
    #Calculate the sector postab within a composite strategy(including Benchmark)
    def Comp_sec_postab(self,postab,primecodelist):
        rebaldaylist=postab['date'].unique()
        sectorstock=self.DC.Sector_stock(rebaldaylist,primecodelist)
        secweight=self.DS.Data_merge(sectorstock,postab,'weight')
        secweight=secweight.loc[secweight['weight']>0]
        postab_sec=self.WS.Generate_PortNav(secweight)
        return(postab_sec)
    
    #Caculate strategy's sector PNL vs a BM's sector PNL
    def SectorPNLvsBM(self,dailyreturn,postabStrat,benchmark,primecodelist):
        startdate=postabStrat['date'].min()
        BM_memb=self.DC.Benchmark_membs(benchmark,startdate)
        #BM_memb=BM_memb.rename(columns={'weight':'PortNav'})
        postabBM=self.Comp_sec_postab(BM_memb,primecodelist)
        SPNL=self.DailyPNL(dailyreturn,postabStrat)
        SPNLBM=self.DailyPNL(dailyreturn,postabBM)
        SPNLBM=SPNLBM.rename(columns={'dailyreturn':'BMdailyreturn'})
        comptab=pd.merge(SPNL,SPNLBM,on='date',how='left')
        comptab['StratCml']=np.exp(np.log1p(comptab['dailyreturn']).cumsum())
        comptab['BMCml']=np.exp(np.log1p(comptab['BMdailyreturn']).cumsum())
        return(comptab)
        
    #For Funda stock, calculate the culmulative return of every Q in every signal 
    def SigcumPNL(self,PNLsigdict):
        keys=list(PNLsigdict.keys())
        PNLcumdict={}
        for keyname in keys:
            SPNL=PNLsigdict[keyname].copy()
            colnamelist=SPNL.columns.tolist()
            cumSPNL=pd.DataFrame(columns=colnamelist)
            cumSPNL['date']=SPNL['date']
            for colname in colnamelist[1:]:
                cumSPNL[colname]=np.exp(np.log1p(SPNL[colname]).cumsum())
            cumSPNL['Alpha']=cumSPNL[5]-cumSPNL[1]
            PNLcumdict[keyname]=cumSPNL
        return(PNLcumdict)
    
    #Input: Postab. It flattens whatever postab to equal weight and calculate P&L
    def EqReturn(self,dailyreturn,postab):
        newpostab=self.WS.Generate_PortNavEqual(postab)
        SPNL=self.DailyPNL(dailyreturn,newpostab)
        CumPNL=self.CumPNL(SPNL)
        return(CumPNL)
    
    #Input the positions table with date, ticker, PortNav%, output every rebalday's turnover in %
    def Turnovercal(df):
        stocklist=list(df['ticker'].unique())
        rebaldaylist=list(df['date'].unique())
        rebaldaytab=[]
        for rebalday in rebaldaylist:
            rebaldaypos=pd.DataFrame({'ticker':stocklist})
            rebaldaypos['date']=rebalday
            rebaldaytab.append(rebaldaypos)
        rebaldaytab=pd.concat(rebaldaytab)
        df['index']=df['date']+df['ticker']
        rebaldaytab['index']=rebaldaytab['date']+rebaldaytab['ticker']
        rebaldaytab=pd.merge(rebaldaytab,df[['index','PortNav%']],on='index',how='left')
        rebaldaytab['PortNav%']=rebaldaytab['PortNav%'].fillna(0)
        rebaldaytab=rebaldaytab.sort_values(by=['ticker','date'],ascending=[True,True]) 
        rebaldaytab['poschange']=rebaldaytab.groupby(['ticker'])['PortNav%'].transform(lambda x: x-x.shift(1))
        rebaldaytab.loc[rebaldaytab['poschange'].isna()==True,'poschange']=rebaldaytab.loc[rebaldaytab['poschange'].isna()==True,'PortNav%']
        rebaldaytab['poschange']=rebaldaytab['poschange'].abs()
        turnoverstats=rebaldaytab.groupby(['date'])['poschange'].sum()
        turnoverstats=turnoverstats.reset_index(inplace=False)
        return(turnoverstats)