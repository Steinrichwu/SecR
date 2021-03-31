# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 09:10:06 2020

@author: wudi
"""


import pandas as pd
from Toolbox import DataCollect 
from MSSQL import MSSQL  

DC=DataCollect()


#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
class Prep():
    def __init__(self):
        pass
    
    #Download data from the Con-sensus database, always the con-estimate of a year ahead of calendar year. 
    def DataExtract(self,rebaldaylist,signame):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="zyyx")
        tradingday=pd.read_csv("D:/SecR/Tradingday.csv")
        loc=tradingday.loc[tradingday['date']==rebaldaylist[0],:].index[0]
        firstday=tradingday.iloc[loc-20,0]
        newrebaldaylist=rebaldaylist.copy()
        newrebaldaylist.insert(0,firstday)
        query="select convert(varchar,con_date,23),stock_code,con_year, "+signame+", con_np_type from zyyx.dbo.con_forecast_stk where con_date in  ("+str(newrebaldaylist)[1:-1]+")"
        reslist=ms.ExecQuery(query)
        df=pd.DataFrame(reslist,columns=['date','ticker','con_year',signame,'con_np_type'])
        df=df.loc[df['con_np_type']==1,:].copy()                #the one with the highest estimates
        df['targetyear']=df['date'].str[0:4]
        df['targetyear']=df['targetyear'].astype(int)+1
        selectdf=df.loc[df['con_year']==df['targetyear'],:].copy()
        selectdf=selectdf.loc[selectdf[signame].isnull()==False,:]
        selectdf=selectdf.loc[selectdf[signame]!=0,:]
        return(selectdf) 
    
    #Forward Value translated the pe/pb/ps...into 1/pe    
    def ForwardValue(self,selectdf,signame):
        selectdf[signame]=1/selectdf[signame]
        return(selectdf)
    
    #the signame is transslated as the Raise of consensus estimates compared with last rebalday
    def Raise(self,selectdf,signame):
        selectdf=selectdf.sort_values(by=['ticker','date'],ascending=[True,True])
        selectdf['diff']=selectdf.groupby(['ticker'])[signame].diff()
        selectdf['shift']=selectdf.groupby(['ticker'])[signame].shift()
        selectdf=selectdf.loc[selectdf['shift'].isnull()==False,:].copy()
        selectdf[signame]=selectdf['diff']/selectdf['shift']
        selectdf=selectdf.dropna()
        return(selectdf)
    
    #Extract data on every rebalday (and one monthe bofore),calculate the raise of 20trading day before
    def Raise_DataExtractList(self,rebaldaylist,signame):
        selectdf=pd.DataFrame()
        for rebalday in rebaldaylist:
            oneselectdf=self.DataExtract([rebalday],signame)
            oneselectdf=self.Raise(oneselectdf,signame)
            if selectdf.shape[0]==0:
                selectdf=oneselectdf.copy()
            else:
                selectdf=selectdf.append(oneselectdf)
        return(selectdf)
    
    
    #It determines what signal and what signame to use, and use different models
    #Signal='Raisecon_or', signame='con_or'/ Signal=Valuecon_pe', signame='con_pe'
    def Consensus(self,signal,rebaldaylist):
        if signal[0:5]=='Value':
            signame=signal[5:]
            selectdf=self.DataExtract(rebaldaylist,signame)
            selectdf=self.ForwardValue(selectdf,signame)
        elif signal[0:5]=='Raise':
            signame=signal[5:]
            selectdf=self.Raise_DataExtractList(rebaldaylist,signame)
        else:
            signame=signal
            selectdf=self.DataExtract(rebaldaylist,signame)
        selectdf=selectdf[['date','ticker',signame]]
        selectdf.columns=['date','ticker','sigvalue']                        
        return(selectdf)
    
