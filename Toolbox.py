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
#Return all stocks' sector information on rebaldays
    def SectorPrep(self,rebaldaylist,publisher):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if publisher=='CITIC':
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        if publisher=='CSI':
            query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=28 and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        tickersectors=pd.DataFrame(reslist,columns=['date','ticker','primecode'])
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
#Enter rebaldaylist,primecodelist, Return stocks of the primecodes on that day
    def Sector_stock(self,rebaldaylist,primecodelist,publisher):
        sector_stock=self.SectorPrep(rebaldaylist,publisher)
        sector_stock=sector_stock.loc[(sector_stock['date'].isin(rebaldaylist))&(sector_stock['primecode'].isin(primecodelist)),['date','ticker']]
        sector_stock=sector_stock.sort_values(by=['date'],ascending=[True])
        return(sector_stock)

#Enter rebaldaylist,primecodelist,Return stocks, their primecode on that day, Keeps Ashs only
    def Ashs_stock_seccode(self,rebaldaylist,primecodelist,publisher):
        sector_stock=self.SectorPrep(rebaldaylist,publisher)
        sector_stock=sector_stock.loc[(sector_stock['date'].isin(rebaldaylist))&(sector_stock['primecode'].isin(primecodelist)),['date','ticker','primecode']]
        sector_stock=sector_stock.sort_values(by=['date'],ascending=[True])
        sector_stock=sector_stock.loc[sector_stock['ticker'].str[0].isin(['6','0','3'])]  
        return(sector_stock)

#Given reablday, return the primecode of all stocks on the list
    def Stock_sector(self,rebaldaylist,tickerlist,publisher):
        stock_sector=self.SectorPrep(rebaldaylist,publisher)
        stock_sector=stock_sector.loc[stock_sector['date'].isin(rebaldaylist),:]
        stock_sector=stock_sector.loc[stock_sector['ticker'].isin(tickerlist),:]
        return(stock_sector)

#Return the list of seccode and secname of CITIC or CSI first level 
    def Sec_name(self,publisher):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        if publisher=='CITIC':
            sql="SELECT distinct LEI.FirstIndustryCode,FirstIndustryName from JYDBBAK.dbo.LC_ExgIndustry LEI where LEI.standard=37"
        if publisher=='CSI':
            sql="SELECT distinct LEI.FirstIndustryCode,FirstIndustryName from JYDBBAK.dbo.LC_ExgIndustry LEI where LEI.standard=28"
        reslist=ms.ExecQuery(sql)
        sec_name=pd.DataFrame(reslist,columns=['sector','sectorname'])
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
        sql="select CODE, rsi_24d from BASIC_PRICE_HIS H where  TIME='"+rebalday+"'"
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
    def FacRollingReturn(self,rolldays):
        Facreturn=pd.read_csv("D:/SecR/FacReturn_CITIC_SIZE.csv")
        FacLogreturn=Facreturn.drop('date',1)
        Facdailyreturn=FacLogreturn.copy()
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
        faclogreturn=self.FacRollingReturn(rolldays)
        primecodelist=[s for s in faclogreturn.columns if s.isdigit()]
        sectorreturn=pd.melt(faclogreturn,id_vars=['date'],value_vars=primecodelist,var_name='primecode',value_name='secrollreturn')
        sectorreturn['index']=sectorreturn['date']+sectorreturn['primecode']
        stock_sector=self.Ashs_stock_seccode(rebaldaylist,primecodelist,'CITIC')
        stock_sector['index']=stock_sector['date']+stock_sector['primecode']
        stock_sector=pd.merge(stock_sector,sectorreturn[['index','secrollreturn']],on='index',how='left')
        stock_sector=stock_sector.dropna()
        stock_sector=stock_sector.drop('index',1)
        return(stock_sector)
    
    #LongandShortterm(use 180+10days cumulative return as signal)
    def LStermSec(self,rebaldaylist):
        sigtab=self.StockSectorCumreturn(rebaldaylist,180)
        sigtab2=self.StockSectorCumreturn(rebaldaylist,10)
        sigtab['index']=sigtab['date']+sigtab['ticker']
        sigtab2['index']=sigtab['date']+sigtab['ticker']
        sigtab=pd.merge(sigtab,sigtab2[['index','secrollreturn']],on='index',how='left')
        sigtab['LTrank']=sigtab['secrollreturn_x'].rank(ascending=True)
        sigtab['STrank']=sigtab['secrollreturn_y'].rank(ascending=True)
        sigtab['momrank']=sigtab['LTrank']+sigtab['STrank']
        sigtab=sigtab[['date','ticker','primecode','momrank']]
        return(sigtab)
    

class WeightScheme():
    def __init__(self):
        self.DC=DataCollect()
    
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
        qlist=list(range(1,6))
        for rebalday in rebaldaylist:
            for q in qlist:
                newqport=olddict[facname+'_'+rebalday].loc[olddict[facname+'_'+rebalday]['Q']==q,['ticker']]
                newqport['date']=rebalday
                newqport['PrtNav%']=1/newqport.shape[0]
                newqportlist=newqport.values.tolist()
                portindex=facname+'_'+str(q)
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