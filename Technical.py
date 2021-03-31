# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:33:17 2021

@author: wudi
"""
from MSSQL import MSSQL
import pandas as pd
import numpy as np
import ta as TA
from Toolbox import DataCollect

DC=DataCollect()

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")

tickerlist=['000300','000951','000952','000849','000928','000929','000930','000931','000932','000933','000935','000936','000937']

class DataCollect():
    def __init__(self):
            pass
#Download the historical price of each sector index
    def Hist_price(self,ticker):
        sql="select convert(varchar,TradingDay,23), ClosePrice,HighPrice, LowPrice from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where TradingDay>'2008-12-31' and SM.SecuCode='"+ticker+"'"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','close','high','low'])
        rechist[['close','high','low']]=rechist[['close','high','low']].astype(float)
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        return(rechist)
    
    #Download day to day return of all sectors
    def Hist_return(self,ticker):
        sql="select convert(varchar,TradingDay,23), SM.SecuCode,  ChangePCT/100 from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where  TradingDay>'2008-12-31' and SM.SecuCode='"+ticker+"'"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        rechist['dailyreturn']=rechist['dailyreturn'].astype('float')
        return(rechist)

class Indicators():
    def __init__(self):
        self.DC=DataCollect()
    
    #Clean up the format of indicator to every indicator
    def Indic_gen(self,histprice,histindic):
        histindic=pd.DataFrame(histindic)
        histindic['date']=histprice['date']
        histindic.columns=(['indic','date'])
        histindic=histindic[['date','indic']]
        return(histindic)
    
    def Onedayback(self,histindic):
        histindic['date']=histindic['date'].shift(-1)
        histindic=histindic.loc[histindic['date'].isnull()==False,:].copy()
        return(histindic)
    
    def MACD(self,ticker):
        histprice=self.DC.Hist_price(ticker)
        diff=TA.trend.macd_diff(histprice['close'])
        diff=self.Indic_gen(histprice,diff)
        diff['MACD_sig']=np.nan
        diff.loc[diff['indic']>0,['MACD_sig']]=1
        diff.loc[diff['indic']<=0,['MACD_sig']]=0
        diff['MACD_sig']=diff['MACD_sig'].fillna(method='ffill')
        diff=self.Onedayback(diff)
        return(diff)
    
    def SMA(self,ticker):
        histprice=self.DC.Hist_price(ticker)
        sma50=TA.trend.sma_indicator(histprice['close'],window=50)
        sma50=self.Indic_gen(histprice,sma50)
        sma50=sma50.rename(columns={'indic':'sma50'})
        sma100=TA.trend.sma_indicator(histprice['close'],window=100)
        sma100=self.Indic_gen(histprice,sma100)
        sma100=sma100.rename(columns={'indic':'sma100'})
        sma=pd.merge(sma50,sma100[['date','sma100']],on='date',how='left')
        sma=sma.loc[sma['sma100'].isnull()==False,:].copy()
        sma['diff']=sma['sma50']-sma['sma100']
        sma['SMA_sig']=np.nan
        sma.loc[sma['diff']>0,['SMA_sig']]=1
        sma.loc[sma['diff']<=0,['SMA_sig']]=0
        sma['diff']=sma['diff'].fillna(method='ffill')
        sma=self.Onedayback(sma)
        return(sma)
    
#Calculate the RSI of each index
    def RSI(self,ticker):
        histprice=self.DC.Hist_price(ticker)
        histindic=TA.momentum.rsi(histprice['close'],window=14)
        histindic=self.Indic_gen(histprice,histindic,ticker)
        histindic['indic']=np.nan
        histindic.loc[histindic['indic']>=70,['indic']]=0
        histindic.loc[histindic['indic']<=50,['indic']]=1
        histindic=histindic.fillna(method='ffill')
        return(histindic)
    
    def ADX(self,histprice):
        histindic=TA.trend.adx(histprice['high'], histprice['low'], histprice['close'],window=14,fillna=False)
        return(histindic)
    
    def Kama(self,histprice):
        histindic=TA.momentum.kama(histprice['close'])
        return(histindic)
    
    def Ppo(self,histprice):
        histindic=TA.momentum.ppo(histprice['close'])
        return(histindic)
    
    def ROC(self,histprice):
        histindic=TA.momentum.ppo(histprice['close'])
        return(histindic)



class Analysis():
    def __init__(self):
        self.DC=DataCollect()
        self.I=Indicators()
    #combine historical price and RSI of each sector 
    #return each sector's RSI or ROC on rebalday 
    #each sector's price (every tradingday)
    def Single_backtest(self,ticker,indicname):
        histindic=getattr(self.I,indicname)(ticker)
        histreturn=self.DC.Hist_return(ticker)
        histreturn=pd.merge(histreturn,histindic,on='date',how='left')
        histreturn=histreturn.sort_values(by=['date'],ascending=[True])
        histreturn['return']=histreturn['dailyreturn']*histreturn[indicname+'_sig']
        histreturn=histreturn.loc[histreturn['return'].isnull()==False,:].copy()
        PNL=histreturn[['date','return']].copy()
        PNL['stratReturn']=np.exp(np.log1p(PNL['return']).cumsum())
        return(histindic,PNL)
        
    
    def Combine(self):
        rebaldaylist=DC.Rebaldaylist('2008-12-31',10)    
        tickerlist=['000951','000952','000849','000928','000929','000930','000931','000932','000933','000935','000936','000937']
        hist_market=self.I.Hist_price('000985')[['date']]
        hist_price=self.I.Hist_price('000985')[['date']]
        for ticker in tickerlist:
            histindic=RSI(ticker)
            hist_market=pd.merge(hist_market,histindic[['date',ticker]],on='date',how='left')
            hist_price=pd.merge(hist_price,histprice[['date',ticker]],on='date',how='left')
        hist_price=hist_price.sort_values(by=['date'],ascending=[True])
        hist_market=hist_market.sort_values(by=['date'],ascending=[True])
        hist_market_indic=pd.melt(hist_market,id_vars='date',value_vars=list(hist_market.columns[1:len(hist_market.columns)+1]),var_name='ticker',value_name='indic')    
        hist_market_indic=hist_market_indic.loc[hist_market_indic['date'].isin(rebaldaylist),:]
        hist_market_indic['indic_rank']=hist_market_indic.groupby("date")["indic"].rank("dense", ascending=False)
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        return(hist_market_indic,hist_price)                        
    
    #Backtest the strategy of buying top3 highest value of the indicator (whatever indicator)
    def Top3_backtest():
        indexdaily=Hist_return() 
        hist_market_indic,hist_price=Combine()
        hist_market_indic=hist_market_indic.loc[hist_market_indic['indic_rank']<=3,].copy() #take the 3sectors with the highest RSI on each rebal day
        hist_market_indic=hist_market_indic.pivot_table(index='date',columns='ticker',values='indic_rank',aggfunc='first')
        hist_market_indic.reset_index(inplace=True)
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        hist_market_indic=hist_market_indic.fillna(0)                                    #Fill na with 0, these are the non top3 sectors on rebal day
        hist_market_indic=pd.merge(hist_price[['date']],hist_market_indic,on='date',how='left')
        hist_market_indic=hist_market_indic.fillna(method='ffill')
        hist_market_indic['date']=hist_market_indic['date'].shift(-1)
        hist_market_indic=hist_market_indic.loc[hist_market_indic['date'].isnull()==False,:].copy()
        hist_market_indic=pd.melt(hist_market_indic,id_vars='date',value_vars=list(hist_market_indic.columns[1:len(hist_market_indic.columns)+1]),var_name='ticker',value_name='indic_rank')
        hist_market_indic=hist_market_indic.loc[hist_market_indic['indic_rank']>0,:].copy()
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        hist_market_indic['index']=hist_market_indic['date']+hist_market_indic['ticker']
        indexdaily['index']=indexdaily['date']+indexdaily['ticker']
        dailyreturn_select=indexdaily.loc[indexdaily['index'].isin(hist_market_indic['index']),:].copy()  #select the dailyreturn of each period's top3 RSI sectors
        dailyreturn_select['dailyreturn']=dailyreturn_select['dailyreturn'].astype(float)
        dailyreturn_select['return']=dailyreturn_select.groupby('date')['dailyreturn'].transform('mean')
        dailyreturn_select=dailyreturn_select.drop_duplicates(subset=['date'],keep='last')
        dailyreturn_select=dailyreturn_select.sort_values(by=['date'],ascending=[True])
        PNL=dailyreturn_select[['date','return']].copy()
        PNL['stratReturn']=np.exp(np.log1p(PNL['return']).cumsum())
        return(PNL)
