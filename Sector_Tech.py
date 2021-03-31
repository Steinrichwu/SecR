# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 16:42:04 2021

@author: wudi
"""
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

#dailyreturn=DC.Dailyreturn_retrieve()
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
#tickerlist=['000300','000951','000952','000849','000928','000929','000930','000931','000932','000933','000935','000936','000937']

class PXDataCollect():
    def __init__(self):
            pass
#Download the historical price of each sector index
    def Hist_sector_return(self,sectortickerlist):
        sql="select convert(varchar,TradingDay,23), SM.SecuCode, OpenPrice,ClosePrice,HighPrice,LowPrice,ChangePCT  from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where TradingDay>'2008-12-31' and SM.SecuCode in ("+str(sectortickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','open','close','high','low','dailychange'])
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        rechist.iloc[:,2:]=rechist.iloc[:,2:].astype('float')
        rechist['dailychange']=rechist['dailychange']/100
        return(rechist)
    
    #Download day to day return & price of stocks
    def Hist_stock_return(self,tickerlist):
        sql="select convert(varchar,TradingDay, 23) as date, SM.SecuCode, OpenPrice,ClosePrice,HighPrice,LowPrice,ChangePCT from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'2006-12-31' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','open','close','high','low','dailychange'])
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        rechist.iloc[:,2:]=rechist.iloc[:,2:].astype('float')
        rechist['dailychange']=rechist['dailychange']/100
        return(rechist)
    
class ClassicIndicators():
    def __init__(self):
        self.DC=DataCollect()
    
    def MACD(self,stockhist):
        macd = pd.DataFrame(TA.trend.macd(stockhist['close'])).dropna()
        macd['date']=stockhist['date']
        macd.loc[macd['MACD_12_26']>0,'signal'] = 1
        macd['signal']=macd['signal'].fillna(0).shift(1)
        return(macd)
        
    def MA(self,stockhist):   #MA
        ma = pd.DataFrame(TA.trend.ema_indicator(stockhist['close'],10))
        stockhist['ema_10']=ma
        stockhist['ema_21'] = TA.trend.ema_indicator(stockhist['close'],21)
        stockhist['ema_68'] = TA.trend.ema_indicator(stockhist['close'],68)
        stockhist['signal']=0
        stockhist.loc[(stockhist['ema_10']>stockhist['ema_21'])&(stockhist['ema_21']>stockhist['ema_68']),'signal']=1
        MA=stockhist[['date','ema_10','ema_21','ema_68','signal']].copy()
        return(MA)
    
        #ADX
    def ADX(self,stockhist):
        adx = pd.DataFrame(TA.trend.adx(stockhist['high'], stockhist['low'], stockhist['close'],window=14,fillna=False))
        stockhist['adx']=adx
        stockhist['signal']=0
        stockhist.loc[stockhist['adx']>20,'signal'] = 1
        ADX=stockhist[['date','adx','signal']].copy()
        return(ADX)
    
        #KDJ
    def KDJ(self,stockhist):
        low_list = stockhist['low'].rolling(9, min_periods=9).min()
        low_list.fillna(value = stockhist['low'].expanding().min(), inplace = True)
        high_list = stockhist['high'].rolling(9, min_periods=9).max()
        high_list.fillna(value = stockhist['high'].expanding().max(), inplace = True)
        rsv = (stockhist['close'] - low_list) / (high_list - low_list) * 100
        KDJ = stockhist.copy()
        KDJ['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        KDJ['D'] = KDJ['K'].ewm(com=2).mean()
        KDJ['J'] = 3 * KDJ['K'] - 2 * KDJ['D']
        KDJ.loc[(KDJ['K']>KDJ['D']),'signal'] = 1
        KDJ['signal'] = KDJ['signal'].fillna(0).shift(1)
        return(KDJ)
        
        #RSI
    def RSI(self,stockhist):
        rsi = stockhist.copy()
        rsi['RSI'] = TA.momentum.rsi(stockhist['close'],14)
        rsi['RSI6'] = TA.momentum.rsi(stockhist['close'],6)
        rsi['RSI24'] = TA.momentum.rsi(stockhist['close'],24)
        rsi.loc[rsi['RSI6']>rsi['RSI24'],'signal'] = 1
        rsi.loc[rsi['RSI6']<rsi['RSI24'],'signal'] = 0
        rsi ['signal']= rsi['signal'].fillna(method = 'ffill').shift(1)
        return()
    
        #CCI
    def CCI(self,stockhist):
        cci=stockhist.copy()
        cci['cci'] = TA.trend.cci(cci['high'], cci['low'], cci['close'], window=10)
        cci['signal'] = np.nan
        cci.loc[cci['cci']>100,'signal'] = 1
        cci['signal'] = cci['signal'].shift(1).fillna(0)
        return()
    
        #AROON
    def AROON(self,stockhist):
        aroon=stockhist.copy()
        aroondown=TA.trend.aroon_down(stockhist['close'],window=20)
        aroonup=TA.trend.aroon_up(stockhist['close'],window=20)
        aroon=pd.DataFrame(aroonup - aroondown)
        aroon['aroon']=aroon
        aroon['signal']=np.nan
        aroon.loc[aroon['signal']>45,'signal'] = 1
        aroon['signal'] = aroon['signal'].shift(1).fillna(0)
        return(aroon)

    
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
        ADX=TA.trend.adx(histprice['high'], histprice['low'], histprice['close'],window=14,fillna=False)
        ADX_NEG=TA.trend.adx_neg(histprice['high'], histprice['low'], histprice['close'],window=14,fillna=False)
        ADX_POS=TA.trend.adx_pos(histprice['high'], histprice['low'], histprice['close'],window=14,fillna=False)
        adx=pd.DataFrame([ADX,ADX_NEG,ADX_POS])
        adx=adx.transpose()
        adx['date']=histprice['date']
        adx['adx_status']='NA'
        adx.loc[adx['adx_pos']>adx['adx_neg'],'adx_status']='bull'
        adx.loc[adx['adx_pos']<adx['adx_neg'],'adx_status']='bear'
        return(adx)
    
    def MACD(self,histprice):
        mdff=TA.trend.macd_diff(histprice['close'])
        masig=TA.trend.macd_signal(histprice['close'])
        macd=pd.DataFrame([mdff,masig])
        macd=macd.transpose()
        macd['date']=histprice['date']
        macd['diff']=macd['mdff']-macd['masig']
        return(macd)
        
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
        self.DC=PXDataCollect()
        self.I=Indicators()
    #combine historical price and RSI of each sector 
    #return each sector's RSI or ROC on rebalday 
    #each sector's price (every tradingday)
    
    def RSI(self):
        rebaldaylist=DC.Rebaldaylist('2008-12-10',10)    
        price=self.DC.Hist_price()
        closepx=price.pivot_table(index='date',columns='ticker',values='close',aggfunc='first')
        closepx.reset_index(inplace=True)
        closepx=closepx.sort_values(by=['date'],ascending=[True])
        tickerlist=list(price['ticker'].unique())
        indictab=closepx.copy()    
        for ticker in tickerlist:
            indictab[ticker]=TA.momentum.rsi(indictab[ticker],window=14)
            #indictab[ticker]=TA.trend.adx(indictab[ticker])
        indictab=indictab.dropna()
        indictab=indictab.sort_values(by=['date'],ascending=[True])
        indictab=pd.melt(indictab,id_vars='date',value_vars=list(indictab.columns[1:len(indictab.columns)+1]),var_name='ticker',value_name='indic')
        indictab=indictab.loc[indictab['date'].isin(rebaldaylist),:]
        indictab['indic_rank']=indictab.groupby("date")["indic"].rank("dense", ascending=False)
        indictab=indictab.sort_values(by=['date'],ascending=[True])
        return(indictab)                        
    
    #Backtest the strategy of buying top3 highest value of the indicator (whatever indicator)
    def Top3_backtest(self,hist_market_indic):
        indexdaily=self.DC.Hist_return()
        tradingday=pd.DataFrame(list(indexdaily['date'].unique()),columns=['date'])
        hist_market_indic=hist_market_indic.loc[hist_market_indic['indic_rank']<=3,].copy() #take the 3sectors with the highest RSI on each rebal day
        hist_market_indic=hist_market_indic.pivot_table(index='date',columns='ticker',values='indic_rank',aggfunc='first')
        hist_market_indic.reset_index(inplace=True)
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        hist_market_indic=hist_market_indic.fillna(0)                                    #Fill na with 0, these are the non top3 sectors on rebal day
        hist_market_indic=pd.merge(tradingday[['date']],hist_market_indic,on='date',how='left')
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
    
    #Make the third ranking RSI the highest, the highest two RSI receive bottom2 ranking
    def flip_ranks(self,hist_market_indic):
        hist_market_indic['highermid']=hist_market_indic.groupby('date')['indic'].transform(lambda x: x.quantile(0.9))
        hist_market_indic['distance']=abs(hist_market_indic['indic']-hist_market_indic['highermid'])
        hist_market_indic['indic_rank']=hist_market_indic.groupby("date")["distance"].rank("dense", ascending=True)
        return(hist_market_indic)
        
    
    #Backtest the strategy of allocating %NAV to sectors index according to its RSI 
    def RSI_backtest(self,hist_market_indic):
        indexdaily=self.DC.Hist_return()
        tradingday=pd.DataFrame(list(indexdaily['date'].unique()),columns=['date'])
        hist_market_indic['PortNav%']=(7-hist_market_indic['indic_rank'])*0.01+0.0769        
        hist_market_indic=hist_market_indic.pivot_table(index='date',columns='ticker',values='PortNav%',aggfunc='first')
        hist_market_indic.reset_index(inplace=True)
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        hist_market_indic=pd.merge(tradingday[['date']],hist_market_indic,on='date',how='left')
        hist_market_indic=hist_market_indic.fillna(method='ffill')
        hist_market_indic['date']=hist_market_indic['date'].shift(-1)
        hist_market_indic=hist_market_indic.loc[hist_market_indic['date'].isnull()==False,:].copy()
        hist_market_indic=pd.melt(hist_market_indic,id_vars='date',value_vars=list(hist_market_indic.columns[1:len(hist_market_indic.columns)+1]),var_name='ticker',value_name='PortNav%')
        hist_market_indic=hist_market_indic.sort_values(by=['date'],ascending=[True])
        hist_market_indic['index']=hist_market_indic['date']+hist_market_indic['ticker']
        indexdaily['index']=indexdaily['date']+indexdaily['ticker']
        dailyreturn_select=pd.merge(indexdaily,hist_market_indic[['index','PortNav%']],on='index',how='left')
        dailyreturn_select['dailyreturn']=dailyreturn_select['dailyreturn'].astype(float)
        dailyreturn_select['return']=dailyreturn_select['dailyreturn']*dailyreturn_select['PortNav%']
        dailyreturn_select['return']=dailyreturn_select.groupby('date')['return'].transform('sum')
        dailyreturn_select=dailyreturn_select.sort_values(by=['date'],ascending=[True])
        dailyreturn_select=dailyreturn_select.drop_duplicates(subset=['date'])
        PNL=dailyreturn_select[['date','return']].copy()
        PNL['stratReturn']=np.exp(np.log1p(PNL['return']).cumsum())
        return(PNL)
    
    #60/20 Strengh作为首要判断指标，稳定。ADI作为次要判断指标，灵敏
    #---xxxxx---#如果是flat，但ADI已经变成了30+，则把状态从flat变成DMI的状态 (The higher of ADX_Pos/ADXNeg)
    #如果要从flat变成Bear/bull,但ADI还没有到30+，则保留原始状态
    #如果要从bear/bull变成flat但ADI还在30+,则保留原始状态
    def Index_envir(self):
        price=self.DC.Hist_price()
        histprice=price.loc[price['ticker']=='000300',:].copy()
        histprice=histprice.sort_values(by=['date'],ascending=[True])
        histprice=histprice.fillna(method='ffill')
        ADX=self.I.ADX(histprice)
        csi300=histprice.copy()
        csi300['ema_long']=TA.trend.ema_indicator(csi300['close'],window=60)
        csi300['ema_short']=TA.trend.ema_indicator(csi300['close'],window=20)
        csi300['ls']=(csi300['ema_short']-csi300['ema_long'])/csi300['ema_long']
        csi300['ss']=(csi300['ema_short']-csi300['ema_short'].shift(3))/csi300['ema_short'].shift(3)
        csi300['strength']=csi300['ls']+csi300['ss']
        csi300['category']=np.nan
        csi300.loc[csi300['strength']>=0.03,'category']='bull'
        csi300.loc[csi300['strength']<=0.03,'category']='flat'
        csi300.loc[csi300['strength']<=-0.03,'category']='bear'
        csi300=csi300.loc[csi300['strength'].isnull()==False,:].copy()
        csi300['sma_long']=csi300['close'].rolling(window=100).mean()
        csi300['sma_short']=csi300['close'].rolling(window=50).mean()
        csi300['long_trend']=csi300['sma_short']-csi300['sma_long']
        csi300.loc[csi300['long_trend']>0,'long_trend']=1
        csi300.loc[csi300['long_trend']<=0,'long_trend']=-1
        csi300=csi300[['date','close','strength','category','long_trend']]
        csi300=pd.merge(csi300,ADX[['date','adx','adx_pos','adx_neg','adx_status']],on=['date'],how='left')
        dfo=csi300.copy()
        df=self.denoising(dfo)
        #df.loc[(df['category']=='flat')&(df['long_trend']==1),'category']='bumpy-up'
        #df.loc[(df['category']=='flat')&(df['long_trend']==-1),'category']='bumpy-down'
        df=df[['date','close','category','adx','long_trend']]
        return(dfo,df)
    
            
    def denoising(self,df):
        for i in range(1,df.shape[0]):
            if (df.loc[i,'category']!='flat') & (df.loc[i-1,'category']=='flat'):
                if(df.loc[i,'adx']<30):
                    df.loc[i,'category']='flat'
            if (df.loc[i,'category']=='flat') &(df.loc[i-1,'category']!='flat'):
                if(df.loc[i,'adx']>30):
                    df.loc[i,'category']=df.loc[i-1,'category']
        return(df)
                    
        
        #df['last_category']=df['category'].shift(1)
        #df.loc[(df['category']=='flat')&(df['adx']>30),'category']=df.loc[(df['category']=='flat')&(df['adx']>30),'adx_status']
        #df['real_change']=''
        #df.loc[(df['last_category']=='flat')&(df['category']!='flat')&(df['adx']<28),'real_change']='fake_move'
        #df.loc[(df['last_category']!='flat')&(df['category']=='flat')&(df['adx']>28),'real_change']='fake_stable'
        #df.loc[df['real_change']=='fake_move','category']=df.loc[df['real_change']=='fake_move','last_category']
        #df.loc[df['real_change']=='fake_stable','category']=df.loc[df['real_change']=='fake_stable','last_category']
        #fake_move_number=len(list(df.loc[df['real_change']!='','real_change']))
        #df=df[['date', 'close', 'strength', 'category', 'adx', 'adx_pos', 'adx_neg','adx_status','long_trend']]
        #return(df,fake_move_number)
        
    
        
        
        