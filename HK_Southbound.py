# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 20:36:45 2021

@author: wudi
"""
import pandas as pd
import numpy as np
from MSSQL import MSSQL
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")

#Southbound Data
hkdata=pd.read_csv("D:/SecR/Southbound.csv")
hkdata['datenew']=pd.to_datetime(hkdata['date'])
hkdata['sharesheld']=hkdata['sharesheld'].astype('float')
hkdata=hkdata.sort_values(by=['ticker','date'],ascending=[True,True])
hkdata['date_diff']=hkdata['datenew']-hkdata['datenew'].shift(1)
hkdata['date_diff'] = pd.to_numeric(hkdata['date_diff'].dt.days, downcast='integer')
hkdata['nthoccur']=hkdata.groupby('ticker').cumcount()+1
hkdata['ticker']=[s[0:5] for s in hkdata['ticker']]
hkdata['index']=hkdata['date']+hkdata['ticker']

#Mcap data
datelist=list(hkdata['date'].unique())
tickerlist=list(hkdata['ticker'].unique())
sql="select TradingDay, SM.SecuCode, NegotiableMV, ClosePrice from JYDBBAK.dbo.QT_HKDailyQuoteIndex  HKQI left join JYDBBAK.dbo.HK_SecuMain SM on HKQI.InnerCode=SM.InnerCode where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and TradingDay>'2017-01-01'"
reslist=ms.ExecQuery(sql)
mcaphist=pd.DataFrame(reslist,columns=['date','ticker','mcap','price'])
mcaphist['date']=mcaphist['date'].astype(str)
mcaphist=mcaphist.loc[mcaphist['date'].isin(datelist),:].copy()
mcaphist['index']=mcaphist['date']+mcaphist['ticker']

#Merge Southbound and Mcap, calculate Notional held by southbound flow: last trading day's price*shares held
hkdata=pd.merge(hkdata,mcaphist[['index','mcap','price']],on='index',how='left')
hkdata['price']=hkdata['price'].astype(float)
hkdata['notionalheld']=hkdata['sharesheld']*hkdata['price']

#Calculate notional increase %
hkdata['holding_increase']=hkdata['notionalheld']/hkdata['notionalheld'].shift(1)-1
hkdata.loc[hkdata['nthoccur']==1,'holding_increase']=np.nan
hkdata.loc[hkdata['date_diff']>45,'holding_increase']=np.nan
hkdata=hkdata.loc[hkdata['date_diff'].isnull()==False,:].copy()
hkdata=hkdata.loc[hkdata['nthoccur']>1,:].copy()

#Universe: marketcap top100 only
hkdata['mcap']=hkdata['mcap'].astype(float)
hkdata['mcaprank']=hkdata.groupby('date')['mcap'].rank("dense", ascending=False)
hkdata=hkdata.loc[hkdata['mcaprank']<=100,:].copy()

#Holding increase: Choose Top30 / equal weighted
hkdata['hold%up_rank']=hkdata.groupby('date')['holding_increase'].rank("dense",ascending=False)
top=30
top30=hkdata.loc[hkdata['hold%up_rank']<=top,:].copy()
top30=top30.sort_values(by=['date'],ascending=True)
top30['PortNav%']=1/top

#Download Daily return of stocks shortlisted
tickerlist=list(top30['ticker'].unique())
sql="select HQ.TradingDay, SM.SecuCode, ChangePCT from JYDBBAK.dbo.QT_HKDailyQuote HQ left join  JYDBBAK.dbo.HK_SecuMain SM on HQ.InnerCode=SM.InnerCode where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and TradingDay>'2017-01-01'"
reslist=ms.ExecQuery(sql)
returntab=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
returntab['dailyreturn']=returntab['dailyreturn']/100
returntab['date']=returntab['date'].astype(str)
tradingday=pd.DataFrame(list(returntab['date'].unique()),columns=['date'])
tradingday['date']=tradingday['date'].astype(str)

#combine the position table with tradingday, move the dates by two days
top30=top30.pivot_table(index='date',columns='ticker',values='PortNav%',aggfunc='first')
top30.reset_index(inplace=True)
top30=top30.fillna(0)
postab=pd.merge(tradingday,top30,on='date',how='left')
postab=postab.sort_values(by=['date'],ascending=True)
postab['date']=postab['date'].shift(-5)
postab=postab.loc[postab['date'].isnull()==False,:].copy()
postab=postab.fillna(method='ffill')
postab=pd.melt(postab,id_vars='date',value_vars=list(postab.columns[1:len(postab.columns)+1]),var_name='ticker',value_name='PortNav%')
postab=postab.loc[postab['PortNav%']>0,:].copy()

#Calculate weighted daily sum of return 
postab['index']=postab['date']+postab['ticker']
returntab['index']=returntab['date']+returntab['ticker']
postab=pd.merge(postab,returntab[['index','dailyreturn']],on='index',how='left')
postab['weightedaily']=postab['PortNav%']*postab['dailyreturn']
postab['weightedsumdaily']=postab.groupby('date')['weightedaily'].transform('sum')

#Cumulative return 
stratreturn=postab.drop_duplicates(subset=['date'],keep='last')
stratreturn=stratreturn.sort_values(by=['date'],ascending=True)
stratreturn=stratreturn[['date','weightedsumdaily']]
stratreturn['cumreturn']=np.exp(np.log1p(stratreturn['weightedsumdaily']).cumsum())