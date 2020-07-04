# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 18:40:08 2020

@author: wudi
"""

import pandas as pd
import numpy as np
from MSSQL import MSSQL     #The SQL connection management tool

#CSI300 historical trading dates and returns
CSI300=pd.read_csv("U:/S/SecR/CSI300.csv")

#Load the historical rolling market cap table
RollingMcap=pd.read_csv("U:/S/SecR/10day_meanMcap.csv")
RollingMcap['ticker']=RollingMcap['ticker'].apply(str)
RollingMcap['ticker']=RollingMcap['ticker'].str.zfill(6)

#Load the sector allocation table
sectoralloc=pd.read_csv("U:/S/SecR/Sector_Allocation.csv")
sectoralloc['seccode']=sectoralloc['seccode'].apply(str)
sectoralloc['allocation']=sectoralloc['allocation'].apply(float)

#Load the stocks' return
dailyreturn=pd.read_csv("U:/S/SecR/DailyReturnNew.csv")

class DataCollect():
    def __init__(self):
        pass
    
    
    #Pull the tickers that have been recommended since 2015
    def Rec_alltickers(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    
        sql="select distinct code from jyzb_new_1.dbo.cmb_report_research where create_date>='2015-01-01'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall()) 
        df.columns=['ticker']
        df=df[df['ticker'].apply(lambda x: len(x)==6)]
        tickerlist=df['ticker'].values.tolist()
        return(tickerlist)
    
    #Download the recommendation history
    def Rec_download(self,startdate,enddate):                                 #Get data from JYDBBAK-（aka.Juyuan-fnundamental)
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    
        sql="select code,into_date,organ_id from jyzb_new_1.dbo.cmb_report_research R left join jyzb_new_1.dbo.I_SYS_CLASS C on C.SYS_CLASS=R.score_id left join jyzb_new_1.dbo.I_ORGAN_SCORE S on S.ID=R.organ_score_id where create_date>='"+startdate+"'"+"and create_date<='"+enddate+"' and (sys_class=7 OR sys_class=5)"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall()) 
        df.columns=['ticker','into_date','firmid']
        df=df[df['ticker'].apply(lambda x: len(x)==6)]
        return(df)  

    
    #Pull a list of Primary/Secondary history secotrs of Citics for all A stocks    
    def Sec_sector(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query="SELECT SecuCode,InfoSource,LEI.Standard,FirstIndustryName, FirstIndustryCode, SecondIndustryName,SecondIndustryCode,LEI.CancelDate FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=3 and SM.SecuCategory=1"
        reslist=ms.ExecNonQuery(query)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['ticker','broker','standard','priname','primcode','secname','seccode','canceldate']
        return(df)
    
    #Download the MarketCap history of all tickers that have recommended history    
    def Marketcap(self,tickerlist):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="select TradingDay, SM.SecuCode, TotalMV from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCode in ('" +"','".join((n for n in tickerlist))+ "')"+" and TradingDay>'2015-01-01'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['date','ticker','TotalMCap']
        return(df)
    
     #Calculate the 10day rolling mean MCap of all tickers in the MCap list:
    def Rolling_Mcap(self,n,df):
        tickerlist=list(set(df['ticker']))
        df['RollMcap']=0
        for ticker in tickerlist:
            #print(ticker)
            df.loc[df['ticker']==ticker,'RollMcap']=df['TotalMCap'].rolling(n,axis=0).mean()      #calculate the N-rolling mean of marketCap
        return(df)
        
    #Calculate Rolling_Mcap
    def Nday_meanMcap(self):
        tickerlist=self.Rec_alltickers()
        df=self.Marketcap(tickerlist)
        Nday_meanMcap=self.Rolling_Mcap(20,df)
        Nday_meanMcap.to_csv("U:/S/SecR/10day_meanMcap.csv",index=False)
        return()
    
    def Return_hist(self):
        tickerlist=self.Rec_alltickers()
        #tickerlist=[x+'.SH' if x.startswith('6') else x+'.SZ' for x in tickerlist]
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="select TradingDay, SM.SecuCode, ChangePCT from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCode in ('" +"','".join((n for n in tickerlist))+ "')"+" and TradingDay>'2014-01-01'"
        #sql="select TIME,CODE,CHANGEPER from BASIC_PRICE_HIS where CODE in('" +"','".join((n for n in tickerlist))+ "')" +" and TIME>='2015-01-01'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['date','ticker','dailyreturn']
        df['dailyreturn']=df['dailyreturn'].apply(float)
        df['dailyreturn']=df['dailyreturn']/100
        df['ticker']=df['ticker'].str.zfill(6)
        df['ticker']=df['ticker'].apply(lambda x:x+'.SH' if x.startswith('6') else x+'.SZ')
        df.to_csv("U:/S/SecR/DailyReturnNew.csv",index=False)
        return()
        

class DataWrangle():
    def __init__(self):
        pass
   
    #Choose the right sector where the company is on the testdate
    def Sector_filter(self,df,date):
        df=df[['ticker','seccode','secname','canceldate']]
        df=df.sort_values(['ticker','canceldate'],ascending=[True,True])
        dfcancel=df.loc[df['canceldate'].isnull(),['ticker','seccode','secname','canceldate']]  #the current sector
        dfafterdate=df.loc[df['canceldate']>=date,:]
        dfafterdate=dfafterdate.drop_duplicates(['ticker'],keep="first") #remove the sectors that appear after the cloeset date to the testdate
        dfsec=pd.concat([dfafterdate,dfcancel])
        dfsec=dfsec.sort_values(['ticker','canceldate'],ascending=[True,True]) 
        dfsec=dfsec.drop_duplicates(['ticker'],keep="first")               #keep the closest date sector and companies that have never changed sector
        dfsec=dfsec[['ticker','seccode','secname']]
        return(dfsec)
        
    #pick the top N stocks of a specific sector on a specific date        
    def Top_Mcap(self,n,df,date,sectorlist):
        sec_topmcap=pd.DataFrame(columns=['date','ticker','TotalMCap','RollMcap','seccode'])
        for sector in sectorlist:
            print(sector)
            dfnew=df.loc[(df['date']==date)&(df['seccode']==sector),:].nlargest(n,columns='RollMcap') #pick the top three stocks of a specific sector on a specific date
            sec_topmcap=pd.concat([sec_topmcap,dfnew])
        return(sec_topmcap)
    
    
    #Decide the %NAV of each of the top3 stock in 
    def Mcap_NAV(self,sec_topmcap):
        sec_topmcap['seccode']=sec_topmcap['seccode'].apply(str)
        sec_topmcap=pd.merge(sec_topmcap,sectoralloc,on='seccode',how='left')
        secnew=sec_topmcap.drop_duplicates(['seccode'],keep="first")
        totalalloc=secnew['allocation'].sum()
        sec_topmcap['secNAV%']=sec_topmcap['allocation']/totalalloc
        sectorlist=sec_topmcap['seccode'].values.tolist()
        sec_topmcap['sum']=0
        sec_topmcap['NAV%']=0
        for sector in sectorlist:
            sec_topmcap.loc[sec_topmcap['seccode']==sector,'sum']=sec_topmcap.loc[sec_topmcap['seccode']==sector,'RollMcap'].sum()
        for sector in sectorlist:
            sec_topmcap.loc[sec_topmcap['seccode']==sector,'NAV%']=sec_topmcap.loc[sec_topmcap['seccode']==sector,'RollMcap']/sec_topmcap.loc[sec_topmcap['seccode']==sector,'sum']
        sec_topmcap['PortNav%']=sec_topmcap['NAV%']*sec_topmcap['secNAV%']
        return(sec_topmcap)
        
            
class Analysis():
    def __init__(self):
        self.DC=DataCollect()
        self.DW=DataWrangle()
    #stats on how times a firm recommended over a period
    #Stats about most recommended sector
    def Recommend_stat(self,startdate,enddate):
        df=self.DC.Rec_download(startdate,enddate)                 #Get all the recommendations of stocks
        print('finished rec_download')
        df.columns=['ticker','date','firm']
        rec_stats=df['ticker'].value_counts().to_frame()           #count how many times recommended
        rec_stats['count']=rec_stats['ticker']
        rec_stats['ticker']=rec_stats.index
        rec_stats=rec_stats.reset_index(drop=True)
        sectorhistory=self.DC.Sec_sector()
        sector=self.DW.Sector_filter(sectorhistory,enddate)
        rec_stats=pd.merge(rec_stats,sector,on='ticker',how='left') #Match the ticker and its sector
        sec_stats=rec_stats['seccode'].value_counts().to_frame()
        sec_stats['count']=sec_stats['seccode']
        sec_stats['seccode']=sec_stats.index
        sec_stats=sec_stats.reset_index(drop=True)
        return(sec_stats)
    
        
        #Pick the top3 MarketCap of each sector in the topsectorlist on a testdate
        #a=DW.Top_Mcap(1,RollingMcap,date,sectorlist)
    def Sec_TopMcap(self,date,sectorlist):
        sectorhistory=self.DC.Sec_sector()
        sector=self.DW.Sector_filter(sectorhistory,date)
        sec_topmcap=pd.merge(RollingMcap,sector[['ticker','seccode']],on='ticker',how='left')
        sec_topmcap=self.DW.Top_Mcap(3,sec_topmcap,date,sectorlist)
        return(sec_topmcap)
    
        #Work out the Names and % on each rebal day
    def Rebal_alloc(self,date,lookback_period):
        enddateloc=CSI300.loc[CSI300['date']==date,:].index[0]
        startdateloc=enddateloc-lookback_period                       #using what period to calculate mean marketcap
        startdate=CSI300.iloc[startdateloc,0]
        enddate=date                                                  #work out the stat period of each rebal day
        sec_stats=self.Recommend_stat(startdate,enddate)              #Stats about most recommended sector
        toptile=np.percentile(sec_stats['count'],70)
        topseccode=sec_stats.loc[sec_stats['count']>=toptile,:]       #Selected the top30% most recommended sectors
        sectorlist=topseccode['seccode'].values.tolist()
        sec_topmcap=self.Sec_TopMcap(date,sectorlist)                 #Put together the top3 marketcap stocks of selected sectors
        sec_topmcap=self.DW.Mcap_NAV(sec_topmcap)                     #Stocks' allocation on rebal day
        return(sec_topmcap)
    
    def Backtest(self,startdate,rebal_period,lookback_period):
        dateloc=CSI300.loc[CSI300['date']==startdate,:].index[0]
        endloc=CSI300.shape[0]
        PNL=pd.DataFrame(columns=['date','dailyreturn'])
        posold=pd.DataFrame(columns=['ticker','PortNav%'])
        while dateloc+rebal_period+1<=endloc:
            date=CSI300.iloc[dateloc,0]
            pos=self.Rebal_alloc(date,lookback_period)
            pos=pos[['ticker','PortNav%']]
            pos['ticker']=pos['ticker'].apply(lambda x:x+'.SH' if x.startswith('6') else x+'.SZ')
            if not posold.empty:
                posnew=pos.append(posold,ignore_index=True,sort=False)
                posnew=pd.pivot_table(posnew,index=['ticker'],values=['PortNav%'],aggfunc=np.sum)
                posnew['ticker']=posnew.index
                posnew['PortNav%']=posnew['PortNav%']/2
                pos=posnew.copy()
            posold=pos.copy()
            pnlstartdate=CSI300.iloc[dateloc+2,0]
            pnlenddate=CSI300.iloc[dateloc+1+rebal_period,0]
            print('BT_startdate'+pnlstartdate)
            print('BT_enddate'+pnlenddate)
            period_return=dailyreturn.loc[(dailyreturn['ticker'].isin(pos['ticker']))&(dailyreturn['date']>=pnlstartdate)&(dailyreturn['date']<=pnlenddate),:]
            period_return_new=period_return.pivot_table(index='date',columns='ticker',values='dailyreturn') 
            period_return_new=period_return_new.fillna(0)
            ticker_intersection=list(set(pos['ticker']) & set(period_return_new.columns)) 
            pos=pos.loc[pos['ticker'].isin(ticker_intersection),:]
            period_return_new=period_return_new[pos['ticker']]                           #Align the %NAV order with PNL table's columns' order
            a=period_return_new.values                                                   #convert into array
            b=pos['PortNav%'].values
            period_return_series=pd.DataFrame(np.dot(a,b),columns=['dailyreturn'])       #PNL=dot product of the arrays
            period_return_series['date']=period_return_new.index
            period_return_series=period_return_series[['date','dailyreturn']]
            PNL=PNL.append(period_return_series,ignore_index=True,sort=False)            #Stack the PNL together
            dateloc=dateloc+rebal_period
        return(PNL)

        
        