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

today=datetime.today() - timedelta(days=1)
rebalday=[str(today)[0:10]]
df=pd.read_csv("D:/SecR/HK_Data.csv")
df=df.dropna()
df['Sector']=df['Sector'].astype(str)
df['MarketCap']=df['MarketCap'].apply(np.log)
df['PE']=1/df['PE']
df['Turnover']=1/df['Turnover']
indu_dummy=pd.get_dummies(df['Sector'])
df=pd.concat([df,indu_dummy],axis=1)
df=df.reset_index(drop=True)
Xset=['MarketCap']
Xset.extend(indu_dummy.columns)
selectsigs=['ROE','SalesGrowth','PE','Turnover']
df.iloc[:,1:]=df.iloc[:,1:].astype(float)
for sig in selectsigs:
    df=DS.Winsorize(df,sig,0.02)
    df[sig]=df[sig].astype(float)
    df[Xset]=df[Xset].astype(float)
    est=sm.OLS(df[sig],df[Xset]).fit()
    df['N_'+sig]=est.resid.values   
dfnew=df[['Ticker','N_ROE','N_SalesGrowth','N_PE','N_Turnover']].copy()
dfnew[['N_ROE','N_SalesGrowth','N_PE','N_Turnover']]=dfnew[['N_ROE','N_SalesGrowth','N_PE','N_Turnover']].apply(lambda x: stats.zscore(x))
dfnew.columns=['ticker','Quality_zscore','Growth_zscore','Value_zscore','Market_zscore']
dfnew.to_csv("D:/CompanyData/CompanyData_HK_"+rebalday[0]+".csv",index=False)