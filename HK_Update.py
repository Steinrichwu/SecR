# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 22:11:36 2020

@author: wudi
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
from Toolbox import DataStructuring 
from scipy import stats
from datetime import datetime, timedelta
DS=DataStructuring()

today=datetime.today()-timedelta(days=1)
rebalday=[str(today)[0:10]]
df=pd.read_csv("D:/SecR/HK_Data.csv")
df=df.dropna()
df['Sector']=df['Sector'].astype(str)
df['MarketCap']=df['MarketCap'].apply(np.log)
df['PE']=1/df['PE']
df['Turnover']=1/df['Turnover']
dffull=df.copy()
dfcut=df['MarketCap'].quantile(0.3)
dfcut=dffull.loc[dffull['MarketCap']>=dfcut,:].copy()

def HK_Analysis(df):
    indu_dummy=pd.get_dummies(df['Sector'])
    df=pd.concat([df,indu_dummy],axis=1)
    df=df.reset_index(drop=True)
    Xset=['MarketCap']
    Xset.extend(indu_dummy.columns)
    selectsigs=['ROE','SalesGrowth','PE','Turnover']
    df.iloc[:,1:]=df.iloc[:,1:].astype(float)
    for sig in selectsigs:
        dfnona=df.loc[df[sig].isna()==False,:].copy()
        dfnona=DS.Winsorize(dfnona,sig,0.02)
        dfnona[sig]=dfnona[sig].astype(float)
        dfnona[Xset]=dfnona[Xset].astype(float)
        est=sm.OLS(dfnona[sig],dfnona[Xset]).fit()
        dfnona['N_'+sig]=est.resid.values
        df=pd.merge(df,dfnona[['Ticker','N_'+sig]],on='Ticker',how='left')
    df=df[['Ticker', 'ROE', 'SalesGrowth', 'PE', 'Turnover', 'MarketCap', 'Sector','N_ROE', 'N_SalesGrowth', 'N_PE', 'N_Turnover']]
    dfnew=df[['Ticker','N_ROE','N_SalesGrowth','N_PE','N_Turnover']].copy()
    for sig in selectsigs:
        dfnona=dfnew.loc[dfnew['N_'+sig].isna()==False,:].copy()
        dfnona[sig+'_zscore']=stats.zscore(dfnona['N_'+sig])
        dfnew=pd.merge(dfnew,dfnona[['Ticker',sig+'_zscore']],on='Ticker',how='left')
    df=pd.merge(df,dfnew[['Ticker','ROE_zscore','SalesGrowth_zscore','PE_zscore','Turnover_zscore']],on='Ticker',how='left')
    df=df.rename(columns={'ROE_zscore':'Quality_zscore'})
    df=df.rename(columns={'SalesGrowth_zscore':'Growth_zscore'})
    df=df.rename(columns={'Value_zscore':'Value_zscore'})
    df=df.rename(columns={'Turnover_zscore':'Market_zscore'})
    return(dfnew,df)

today=datetime.today()
todayname=str(today.strftime("%Y-%m-%d"))
dfnew,df=HK_Analysis(dffull)
#dfnew.to_csv("D:/CompanyData/CompanyDataFullUniverse_HK_"+todayname+".csv",index=False)
df.to_csv("D:/CompanyData/GentableFullUniverse_HK_"+todayname+".csv",index=False)

dfnew,df=HK_Analysis(dfcut)
df.to_csv("D:/CompanyData/Gentable_HK_"+todayname+".csv",index=False)