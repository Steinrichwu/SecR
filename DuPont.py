# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 15:27:32 2020

@author: wudi
"""


import pandas as pd
import numpy as np
from Toolbox import DataCollect
from Toolbox import WeightScheme
from MSSQL import MSSQL
from Toolbox import ReturnCal
from Toolbox import DataStructuring 
from Querybase import Query
from functools import reduce

DC=DataCollect()
RC=ReturnCal()
DS=DataStructuring()
Q=Query()
WS=WeightScheme()

class Prep():
    def __init__(self):
        pass
    
    def Sumcolumns(self,df):
        colnames=list(df.columns)
        colnames=[x for x in colnames if x not in ['publdate','enddate','ticker']]
        df['sum']=df[colnames].sum(axis=1)
        return(df)
    
    #15/16:金融资产/金融负债
    def FinancialRatios(self,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query=getattr(Q,'FinancialLiability')(startdate)
        reslist=ms.ExecQuery(query)
        FLiability=pd.DataFrame(reslist,columns=['publdate', 'enddate','ticker', 'FLiability'])
        query=getattr(Q,'FinancialAsset')(startdate)
        reslist=ms.ExecQuery(query)
        FAsset=pd.DataFrame(reslist,columns=['publdate', 'enddate','ticker', 'TradingAssets', 'HoldForSaleAssets', 'HoldToMaturityInvestments', 'OthEquityInstrument', 'OthNonCurFinAssets', 'DebtInvestment','OthDebtInvestment'])
        FAsset=self.Sumcolumns(FAsset)
        FAsset=FAsset.rename(columns={'sum':'FAsset'})
        FAsset=FAsset[['publdate','enddate','ticker','FAsset']]
        return(FAsset,FLiability) 
    
    #12/13 总资产/总负债
    def TotalRatios(self,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query=getattr(Q,'TotalAsset')(startdate)
        reslist=ms.ExecQuery(query)
        TAsset=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','TAsset'])
        query=getattr(Q,'TotalLiability')(startdate)
        reslist=ms.ExecQuery(query)
        TLiability=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','TLiability'])
        return(TAsset,TLiability)
    
    def PNLTTM(self,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query=getattr(Q,'PNLTTM')(startdate)
        reslist=ms.ExecQuery(query)
        PNLTTM=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','ORTTM', 'OPTTM','FExpTTM'])
        return(PNLTTM)
    
    def TotalEquity(self,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query=getattr(Q,'TotalEquity')(startdate)
        reslist=ms.ExecQuery(query)
        TotalEquity=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','TotalEquity'])
        return(TotalEquity)
    
    def Cleanup(self,df):
        df=df.loc[df['ticker'].str[0].isin(['6','0','3'])].copy() 
        df['publdate']=df['publdate'].astype(str)
        df['enddate']=df['enddate'].astype(str)
        df=df.sort_values(by=['publdate','enddate'],ascending=[True,True])
        df=df.drop_duplicates(subset=['enddate','ticker'],keep='first')
        df['index']=df['publdate']+df['enddate']+df['ticker']
        df=df.drop(['publdate','enddate','ticker'],axis=1)
        return(df)

class Structuring():
    def __init__(self):
        self.P=Prep()
    
    def DuPontTab(self,startdate):
        print('downloading Fianancials')
        FAsset,FLiability=self.P.FinancialRatios(startdate)
        print('downloading Total')
        TAsset,TLiability=self.P.TotalRatios(startdate)
        print('downloading TTM')
        PNLTTM=self.P.PNLTTM(startdate)
        print('downloading Equity')
        TotalEquity=self.P.TotalEquity(startdate)
        print('download done')
        FAsset,FLiability,TAsset,TLiability,PNLTTM,TotalEquity=map(self.P.Cleanup,(FAsset,FLiability,TAsset,TLiability,PNLTTM,TotalEquity))
        data_frames=[TAsset,TLiability,FAsset,FLiability,PNLTTM,TotalEquity]
        dupontmerged=reduce(lambda left,right: pd.merge(left,right, on=['index'],how='left'),data_frames)
        dupontmerged['publdate']=dupontmerged['index'].str[0:10]
        dupontmerged['enddate']=dupontmerged['index'].str[10:20]
        dupontmerged['ticker']=dupontmerged['index'].str[20:26]
        dupontmerged=dupontmerged.drop(['index'],axis=1)
        return(dupontmerged)
    
    def DuPontCal(self,dupontmerged):
        dp=dupontmerged.copy()
        dp['OpAsset']=dp['TAsset']-dp['FAsset']
        dp['OpLiability']=dp['TLiability']-dp['FLiability']
        dp['NetFLiability']=dp['FLiability']-dp['FAsset']
        dp['NetOpAsset']=dp['OpAsset']-dp['OpLiability']
        dp['opmargin']=dp['OPTTM']/dp['ORTTM']
        dp['OpAssetTurnover']=dp['ORTTM']/dp['NetOpAsset']
        dp['NetOpAssetMargin']=dp['OPTTM']/dp['NetOpAsset']
        dp['AfterTaxIntRate']=dp['FExpTTM']/dp['NetFLiability']
        dp['OpDiffRatio']= dp['NetOpAssetMargin']- dp['AfterTaxIntRate']
        dp['NetLev']=dp['NetFLiability']/dp['TotalEquity']
        return(dp)