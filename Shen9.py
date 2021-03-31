# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 14:23:36 2021

@author: wudi
"""
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
"""
n's position will be reflected in n+1 P&L
shift(1): move to one row lower
"""
from MSSQL import MSSQL
import pandas as pd
import numpy as np
import ta as TA
import talib as tal
from Toolbox import DataCollect
from Toolbox import DataStructuring
from Toolbox import ReturnCal


DC=DataCollect()
DS=DataStructuring()
RC=ReturnCal()
#dailyreturn=DC.Dailyreturn_retrieve()
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
#tickerlist=['000300','512070','000928','000930','000931','000932','000933','000935','000936','000937','000951','000952','000929']

class PXDataCollect():
    def __init__(self):
            pass
#Download the historical price of each sector index
    def Hist_sector_return(self,sectortickerlist):
        sql="select convert(varchar,TradingDay,23), convert(varchar,SM.SecuCode,23), OpenPrice,ClosePrice,HighPrice,LowPrice,ChangePCT,TurnoverVolume from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where TradingDay>'2008-12-31' and SM.SecuCode in ("+str(sectortickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','open','close','high','low','dailyreturn','volume'])
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        rechist.iloc[:,2:]=rechist.iloc[:,2:].astype('float')
        rechist['dailyreturn']=rechist['dailyreturn']/100
        return(rechist)
    
    #Download day to day return & price of stocks
    def Hist_stock_return(self,tickerlist):
        sql="select convert(varchar,TradingDay, 23) as date, convert(varchar,SM.SecuCode,23), OpenPrice,ClosePrice,HighPrice,LowPrice,ChangePCT,TurnoverVolume from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'2006-12-31' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','open','close','high','low','dailyreturn','volume'])
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        rechist.iloc[:,2:]=rechist.iloc[:,2:].astype('float')
        rechist['dailyreturn']=rechist['dailyreturn']/100
        return(rechist)
    
    #depends on switch=sector or indexcomponent, choose the right tickers to download data
    def Get_asset_hist(self,switch,dailyreturn):
        if switch=='sector':
            tickerlist=['000300','000928','000930','000931','000932','000933','000934','000935','000936','000937','000951','000952','000929']
            rechist=self.Hist_sector_return(tickerlist)
            rechist=rechist.rename(columns={'volume':'dailyvolume'})
            BM_memb=rechist.loc[rechist['date']=='2009-09-22',['date','ticker']].copy()
            rechist
        if switch=='indexcomponent':
            BM_memb=DC.Benchmark_membs('CSI800','2009-01-01')
            tickerlist=list(BM_memb['ticker'].unique())
            rechist=dailyreturn.loc[(dailyreturn['ticker'].isin(tickerlist))&(dailyreturn['date']>'2009-01-01'),['date', 'ticker', 'closeprice', 'dailyreturn', 'dailyvolume']].copy() #price history of stocks in the tickerlist
            rechist=rechist.rename(columns={'closeprice':'close'})        
        return(rechist,BM_memb)
    

class Variable_Indicators():
    def __init__(self):
        self.DC=DataCollect()
        
    def MA(self,stockhist,longN,shortN):
        ma=stockhist.copy()
        ma['ema_long'] = pd.DataFrame(TA.trend.ema_indicator(ma['close'],longN))
        ma['ema_short'] = TA.trend.ema_indicator(ma['close'],shortN)
        ma[str(shortN)+'-'+str(longN)]=ma['ema_short']-ma['ema_long']
        ma=ma[['date',str(shortN)+'-'+str(longN)]].copy()
        return(ma)
        
    
class Indicators():
    def __init__(self):
        self.DC=DataCollect()
        
        #MACD
    def MACD(self,stockhist):
        macd=stockhist.copy()
        macd['macd'] = pd.DataFrame(TA.trend.macd(macd['close']))
        macd.loc[macd['macd']>0,'signal'] = 1
        macd['signal']=macd['signal'].fillna(0)
        macd=macd[['date','dailyreturn','signal']].copy()
        return(macd)
        
        #MA
    def MA(self,stockhist):   
        ma=stockhist.copy()
        ma['ema_10'] = pd.DataFrame(TA.trend.ema_indicator(ma['close'],10))
        ma['ema_21'] = TA.trend.ema_indicator(ma['close'],21)
        ma['ema_68'] = TA.trend.ema_indicator(ma['close'],68)
        ma['signal']=0
        ma.loc[(ma['ema_10']>ma['ema_21'])&(ma['ema_21']>ma['ema_68']),'signal']=1
        ma=ma[['date','dailyreturn','signal']].copy()
        return(ma)
    
        #ADX
    def ADX(self,stockhist):
        adx=stockhist.copy()
        adx['adx'] = pd.DataFrame(TA.trend.adx(adx['high'], adx['low'], adx['close'],window=14,fillna=False))
        adx['signal']=0
        adx.loc[adx['adx']>20,'signal'] = 1
        adx=adx[['date','dailyreturn','signal']].copy()
        return(adx)
    
        #KDJ
    def KDJ(self,stockhist):
        KDJ = stockhist.copy()
        low_list = KDJ['low'].rolling(9, min_periods=9).min()
        low_list.fillna(value = KDJ['low'].expanding().min(), inplace = True)
        high_list = KDJ['high'].rolling(9, min_periods=9).max()
        high_list.fillna(value = KDJ['high'].expanding().max(), inplace = True)
        rsv = (KDJ['close'] - low_list) / (high_list - low_list) * 100
        KDJ['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        KDJ['D'] = KDJ['K'].ewm(com=2).mean()
        KDJ['J'] = 3 * KDJ['K'] - 2 * KDJ['D']
        KDJ.loc[(KDJ['K']>KDJ['D']),'signal'] = 1
        KDJ['signal'] = KDJ['signal'].fillna(0)
        KDJ=KDJ[['date','dailyreturn','signal']].copy()
        return(KDJ)
        
        #RSI
    def RSI(self,stockhist):
        rsi = stockhist.copy()
        rsi['RSI'] = TA.momentum.rsi(stockhist['close'],14)
        rsi['RSI6'] = TA.momentum.rsi(stockhist['close'],6)
        rsi['RSI24'] = TA.momentum.rsi(stockhist['close'],24)
        rsi.loc[rsi['RSI6']>rsi['RSI24'],'signal'] = 1
        rsi.loc[rsi['RSI6']<rsi['RSI24'],'signal'] = 0
        rsi['signal']= rsi['signal'].fillna(method = 'ffill')
        rsi=rsi[['date','dailyreturn','signal']].copy()
        return(rsi)
    
        #CCI
    def CCI(self,stockhist):
        cci=stockhist.copy()
        cci['cci'] = TA.trend.cci(cci['high'], cci['low'], cci['close'], window=10)
        cci['signal'] = np.nan
        cci.loc[cci['cci']>100,'signal'] = 1
        cci['signal'] = cci['signal'].fillna(0)
        cci=cci[['date','dailyreturn','signal']].copy()
        return(cci)
    
        #AROON
    def AROON(self,stockhist):
        aroon=stockhist.copy()
        aroondown=TA.trend.aroon_down(stockhist['close'],window=20)
        aroonup=TA.trend.aroon_up(stockhist['close'],window=20)
        aroon['aroon']=pd.DataFrame(aroonup - aroondown)
        aroon['signal']=np.nan
        aroon.loc[aroon['aroon']>45,'signal'] = 1
        aroon['signal'] = aroon['signal'].fillna(0)
        aroon=aroon[['date','dailyreturn','signal']].copy()
        return(aroon)
    
        #CMO
    def CMO(self, stockhist):
        cmo=stockhist.copy()
        cmo['cmo']=tal.CMO(stockhist['close'])
        cmo['signal']=0
        cmo.loc[cmo['cmo']>25,'signal']=1
        cmo['signal']=cmo['signal'].fillna(0)
        cmo=cmo[['date','dailyreturn','signal']].copy()
        return(cmo)

        #BOLL
    def BOLL(self, stockhist):
        boll=stockhist.copy()
        boll['upper'], boll['middle'], boll['lower'] = tal.BBANDS(boll['close'],timeperiod=20,nbdevup=2,nbdevdn=2, matype=0)
        boll['signal']=0
        boll.loc[boll['close']>boll['upper'],'signal'] = 0
        boll.loc[boll['close']<boll['lower'],'signal'] = 1
        boll['signal']=boll['signal'].fillna(method = 'ffill')
        boll=boll[['date','dailyreturn','signal']].copy()
        return(boll)
    
        #TRIX
    def TRIX(self,stockhist):
        trix=stockhist.copy()
        trix['trix']=tal.TRIX(trix['close'],timeperiod=15) # 计算TRIX
        trix['matrix']=tal.MA(trix['trix'],45,0)
        trix['signal']=0
        trix.loc[trix['trix']>trix['matrix'],'signal']=1
        trix.loc[trix['trix']<=trix['matrix'],'signal']=0
        trix['signal']=trix['signal'].fillna(method='ffill')
        trix=trix[['date','dailyreturn','signal']].copy()
        return(trix)
 
        #DMI
    def DMI(self,stockhist):
        dm=stockhist.copy()
        ADX=tal.ADX(dm['high'], dm['low'], dm['close'],timeperiod=14)
        ADX_NEG=tal.MINUS_DI(dm['high'], dm['low'], dm['close'],timeperiod=14)
        ADX_POS=tal.PLUS_DI(dm['high'], dm['low'], dm['close'],timeperiod=14)
        adx=pd.DataFrame([ADX,ADX_NEG,ADX_POS])
        adx=adx.transpose()
        adx.columns=['adx','adx_pos','adx_neg']
        adx['date']=stockhist['date']
        adx.loc[adx['adx_pos']>adx['adx_neg'],'signal']=1
        adx.loc[adx['adx_pos']<adx['adx_neg'],'signal']=0
        dm=pd.merge(dm,adx[['date','signal']],on='date',how='left')
        dm['signal']=dm['signal'].fillna(method='ffill')
        dm=dm[['date','dailyreturn','signal']].copy()
        return(dm)
    
        #Chaikin
    def Chaikin(self,stockhist):
        chaikin=stockhist.copy()
        chaikin['chaikin']=tal.ADOSC(stockhist['high'],stockhist['low'],stockhist['close'],stockhist['volume'],fastperiod=30,slowperiod=60)
        chaikin.loc[chaikin['chaikin']>0,'signal']=1
        chaikin.loc[chaikin['chaikin']<=0,'signal']=0
        chaikin['signal']=chaikin['signal'].fillna(method='ffill')
        chaikin=chaikin[['date','dailyreturn','signal']].copy()
        return(chaikin)
    
        #EMV
    def EMV(self,stockhist):
        emv=stockhist.copy()
        emv['emv']=TA.volume.ease_of_movement(emv['high'], emv['low'], emv['volume'],window=14)
        emv.loc[emv['emv']>0,'signal']=1
        emv.loc[emv['emv']<=0,'signal']=0
        emv['signal']=emv['signal'].fillna(method='ffill')
        emv=emv[['date','dailyreturn','signal']].copy()
        return(emv)

    
class Period_Return():
    def __init__(self):
        self.PDC=PXDataCollect()
        self.I=Indicators()
    #combine historical price and RSI of each sector 
    #return each sector's RSI or ROC on rebalday 
    #each sector's price (every tradingday)
    
    #计算持仓时期的max return....这个max测量范围为这一期持仓到目前已经过去了多少天
    def LookbackMax(self,row,values,lookbackref,method='max',*args,**kwargs):
        loc=values.index.get_loc(row.name)
        pl=lookbackref.loc[row.name]
        period_max=getattr(values.iloc[loc-(pl-1):(loc+1)],method)(*args, **kwargs)
        return(period_max)
    
    #test['new_col']=test.apply(Lookback,values=test['culreturn'],lookbackref=test['window'],axis=1)
        
        #区间累计回报
    def Period_culreturn(self,stockhist_signal):
        #testfile=pd.read_csv("D:/MISC/test.csv")           #导入历史回报
        stockhist_signal['poschange']=stockhist_signal['position']-stockhist_signal['position'].shift(1)   #把买入点标记出来，1为买入，-1为卖出
        stockhist_signal.loc[stockhist_signal['poschange']==-1,'poschange']=0                      #只保留买入点
        if stockhist_signal['position'][0]==1:
            stockhist_signal.loc[0,'poschange']=1
        else:
            stockhist_signal.loc[0,'poschange']=0                                          #如果表格第一天没持仓，则定性为0，有持仓为1
        stockhist_signal['dailyreturn']=stockhist_signal['dailyreturn'].astype('float')
        stockhist_signal['culreturn']=np.exp(np.log1p(stockhist_signal['dailyreturn']).cumsum())   #计算历史cumulative return
        stockhist_signal['trade_cul']=stockhist_signal['poschange']*stockhist_signal['culreturn']          #用0/1去乘cumulative return，只有买入那天的cumulative return会被保留；其余为0
        stockhist_signal['trade_cul']=stockhist_signal['trade_cul'].replace(to_replace=0,method='ffill') #把0换成last return. 则这一列全都是上一次买入那天当天的cumulative return
        stockhist_signal['period_culreturn']=stockhist_signal['culreturn']/stockhist_signal['trade_cul']-1        #用截止今天cumulativereturn/上一次买入当天的cumulative return就是持仓这段时间的cumulative return啦；再减1就是drawdown
        stockhist_signal['position2']=stockhist_signal['position'].shift(1)                              #因为是收盘下单，所以把标记往后挪一天
        stockhist_signal.loc[stockhist_signal['position2']==0,'period_culreturn']=0                       #不持仓的时候最大跌幅变为0
        stockhist_signal=stockhist_signal[['date','dailyreturn','position','period_culreturn']]           
        return(stockhist_signal)
        
        #区间最大回撤
    def Period_highdd(self,stockhist_signal):
        #testfile=pd.read_csv("D:/MISC/test.csv")                                                   #导入历史回报
        stockhist_signal['cumpos']=stockhist_signal['position'].cumsum()                            #把所有position的0/1都叠加起来
        stockhist_signal['culreturn']=np.exp(np.log1p(stockhist_signal['dailyreturn']).cumsum())   #计算历史cumulative return
        stockhist_signal['lastcum']=np.nan                                                  
        stockhist_signal['temp']=stockhist_signal['cumpos'].shift(1)                            
        stockhist_signal.loc[stockhist_signal['position']==0,'lastcum']=stockhist_signal.loc[stockhist_signal['position']==0,'temp'] #lastcum=上期持仓结束的时候累计总天数
        stockhist_signal.loc[0,'lastcum']=0                                                   
        stockhist_signal['lastcum']=stockhist_signal['lastcum'].fillna(method='ffill')              #如果今天不持仓，上期关仓时的累计天数为t-1累计天数；如果今天持仓。上期关仓累计天数跟着last row即可
        stockhist_signal['period_cum']=stockhist_signal['cumpos']-stockhist_signal['lastcum']               #本期开始到现在已经持仓了多少天=本期总累计天数-上期持仓结束前的累计总天数    
        stockhist_signal['period_cum']=stockhist_signal['period_cum'].astype(int)
        stockhist_signal['period_maxcul']=stockhist_signal.apply(self.LookbackMax,values=stockhist_signal['culreturn'],lookbackref=stockhist_signal['period_cum'],axis=1)
        stockhist_signal['period_highdd']=stockhist_signal['culreturn']/stockhist_signal['period_maxcul'].shift(1)-1
        stockhist_signal=stockhist_signal.drop(['cumpos','culreturn','lastcum','temp','period_cum','period_maxcul'],1)
        return(stockhist_signal)

class Analysis():
    def __init__(self):
        self.PDC=PXDataCollect()
        self.PR=Period_Return()
        self.I=Indicators()
        self.VI=Variable_Indicators()
    
    #IF n's maxdrawdown<-20% and n's culreturn<-5%, then n's position should be reverted to 0; n's position will be reflected in n+1 P&L
    #Based on maxdd and culmulative return, alter the position to new 0/1
    #当天position指的是当天收盘的position, P&L由第二天体现（* t+1的dailyreturn)
    def Cutloss(self,stockhist_signal):
        cutloss=stockhist_signal.copy()
        cutloss['adjpos']=np.nan
        cutloss.loc[(cutloss['period_highdd']<-0.20)|(cutloss['period_culreturn']<-0.05),'adjpos']=0  #超过止损线当天的adjposition就会变成0，
        cutloss.loc[cutloss['position']==0,'adjpos']=0                                                #原来已经不持仓的日子，adjposition就是0
        cutloss['openpos']=cutloss['position']-cutloss['position'].shift(1)                           #辩别历史上从0变成1的开仓日，adjposition标记为1
        if cutloss['position'][0]==1:                                                                 #处理数列第一日应该为0还是1
            cutloss.loc[0,'openpos']=1
        else:
            cutloss.loc[0,'openpos']=0             
        cutloss.loc[cutloss['openpos']==1,'adjpos']=1
        cutloss['adjpos']=cutloss['adjpos'].fillna(method='ffill')                                    #按上一步填充，就是调整后的仓位：adjposition
        return(cutloss)
    
    
    #Choose the topn signals in the past 500 trading days, return a list of top3 indicators
    #Calculate total holding days in each period (bettimes) & cumulative return in each period
    #cumulative return/holding days=>accuracy of holding period
    #However, we still use the period cumulative return to rank topn
    def Cumreturn_Adj(self,signalperf,signallist,rebaldaylist,topn):
        cumtab=signalperf[signallist].copy()
        postab=cumtab.copy()
        postab=postab.fillna(0)
        postab[postab!=0]=1
        postab=postab.cumsum()
        postab['date']=signalperf['date']
        postab=postab.loc[postab['date'].isin(rebaldaylist),:].copy()          #only rebalday's cumreturn will be selected, hence divided by last one will be the preodic cumulative return
        postab[signallist]=postab[signallist]-postab[signallist].shift(8)      #total positions in the past two years
        postab=pd.melt(postab,id_vars=['date'],value_vars=signallist,var_name='signame',value_name='bettimes')
        postab=postab.dropna(subset=['bettimes'])                              #Postab counts how many days of taking positions in every period
        postab.loc[postab['bettimes']==0,'bettimes']=1                         #If bet 0 days, convert it to 1 day.
        postab['index']=postab['date']+postab['signame']                       #Postab returns the net holding days of each period (bettimes)
        cumtab=cumtab.apply(lambda x: np.exp(np.log1p(x).cumsum()),axis=0)     #total cumulative return of diff signals
        cumtab['date']=signalperf['date']                             
        cumtab=cumtab.loc[cumtab['date'].isin(rebaldaylist),:].copy()          #take rebalance days only
        cumtab[signallist]=cumtab[signallist]/cumtab[signallist].shift(8)-1    #return cumulative return of past two years, ie, 8 perios 
        cumtab=pd.melt(cumtab,id_vars=['date'],value_vars=signallist,var_name='signame',value_name='cumreturn')
        cumtab=cumtab.dropna(subset=['cumreturn'])                                          #Drop NA, the first rebal day must be NA, those without datafeed would also be NA
        cumtab['index']=cumtab['date']+cumtab['signame']
        cumtab=pd.merge(cumtab,postab[['index','bettimes']],on='index',how='left')          
        cumtab['accuracy']=cumtab['cumreturn']/cumtab['bettimes']                           #Each period's cumreturn/bettimes is 持仓期内的平均回报accuracy
        cumtab['rank']=cumtab.groupby(['date'])['cumreturn'].rank("dense", ascending=False) #give a rank to the cumulative return of last period's (2years) cumulative return
        cumtab=cumtab.loc[cumtab['rank']<=topn,:].copy()                                    #Keep only the topn signals
        return(cumtab)
        
        
    #For Every ticker, choose the top3 that performed best in the past 2 years    
    #Return cumtab: the shortlisted signals and its ranking on each rebal day; SignalPerf: the tab of all signal's adjusted performance
    def IndicSelect(self,stockhist,rebaldaylist,signallist,topn):
        signalperf=pd.DataFrame(stockhist.loc[70:,'date'])
        for signal in signallist:
            print(signal)
            stockhist_signal=getattr(self.I,signal)(stockhist)                      #return 0/1 signal on the stock 
            stockhist_signal['dailysigreturn']=stockhist_signal['dailyreturn']*stockhist_signal['signal'].shift(1) #the dailyreturn after applying the signal
            stockhist_signal=stockhist_signal.iloc[70:,:].copy()                                 #take the section with valid signals only
            stockhist_signal=stockhist_signal.reset_index(drop=True)
            stockhist_signal=stockhist_signal.rename(columns={'signal':'position'})
            stockhist_signal=self.PR.Period_culreturn(stockhist_signal)                          #Add a column of periodic cumulative return
            stockhist_signal=self.PR.Period_highdd(stockhist_signal)                             #Add a column of periodic high drawdown
            stockhist_signal=self.Cutloss(stockhist_signal)                                      #Applied cutloss and change pos column accordingly/temp column is the adjusted position
            stockhist_signal['adjdailyreturn']=stockhist_signal['dailyreturn']*stockhist_signal['adjpos'].shift(1) #Adjdailyreturn is the dailyreturn AFTER taking into account the cutloss positions
            stockhist_signal.loc[0,'adjdailyreturn']=0
            stockadjreturn=stockhist_signal[['date','adjdailyreturn']].copy()
            stockadjreturn=stockadjreturn.rename(columns={'adjdailyreturn':signal})
            signalperf=pd.merge(signalperf,stockadjreturn,on='date',how='left')
        cumtab=self.Cumreturn_Adj(signalperf,signallist,rebaldaylist,topn)
        signalperf=signalperf.sort_values(by=['date'],ascending=[True])
        return(cumtab,signalperf)
    
    #According to period return/or accuracy of the TopN signal, allocate weights to the signals:
    def Signal_weight(self,cumtab):
        cumtab['sumcumreturn']=cumtab.groupby(['date'])['cumreturn'].transform('sum')
        cumtab['weight']=cumtab['cumreturn']/cumtab['sumcumreturn']
        return(cumtab)
        
    #Calculate the cumulative return of the top n signals (mean)
    def CombineTopSignals(self,cumtab,signalperf):
        cumtab=cumtab.pivot_table(index='date',columns='signame',values='weight',aggfunc='first') #make the weight a pivot table 
        cumtab=cumtab.fillna(0)                                                               #if it used to be in topn signals, but not anymore, it will have 0 weight    
        cumtab.reset_index(inplace=True)
        cumtab2=pd.DataFrame(signalperf.loc[signalperf['date']>=cumtab['date'].min(),'date']) #make dates=trading days
        cumtab2=pd.merge(cumtab2,cumtab,on='date',how='left')                         #combine tradingday with pivot table of signal weight
        cumtab2=cumtab2.fillna(method='ffill')                                        #fillna with last rebal, if last rebal is 0, then it's 0 weight
        cumtab2=pd.melt(cumtab2,id_vars='date',value_vars=list(cumtab2.columns[1:len(cumtab2.columns)+1]),var_name='signal',value_name='weight')
        cumtab2['index']=cumtab2['date']+cumtab2['signal']
        signalperfds=pd.melt(signalperf,id_vars='date',value_vars=list(signalperf.columns[1:len(signalperf.columns)+1]),var_name='signal',value_name='adjdailyreturn')
        signalperfds['index']=signalperfds['date']+signalperfds['signal']
        signalperfds['shift_adjdailyreturn']=signalperfds['adjdailyreturn'].shift(-1) #dailyreturn is underlier's reutn, adjdailyreturn is the next day's return after applying signals
        cumtab2=pd.merge(cumtab2,signalperfds[['index','adjdailyreturn','shift_adjdailyreturn']],on='index',how='left')
        cumtab2['pos']=0
        cumtab2.loc[cumtab2['shift_adjdailyreturn']!=0,'pos']=1                      #use next day's historical signal perf table to check which topn signal has 0/1 (if return!=0, it means it has 1 pos)
        cumtab2['weighted_pos']=cumtab2['weight']*cumtab2['pos']                     #weighted_pos=weight of signal* signal's 0/1
        cumtab2['gen_pos']=cumtab2.groupby(['date'])['weighted_pos'].transform('sum')
        cumtab2.loc[cumtab2['gen_pos']>1,'gen_pos']=1                                 #if total position weight>1, then shrink it back to 1
        cumtab2=cumtab2.drop_duplicates(subset=['date'])
        return(cumtab2)
    
    #Return the sigdiff of different frequency range, standardized by spot price, the higher the sifdiff, the higher the momentum
    def Clustering(self,signal,stockhist,freqlist):
        clustertab=pd.DataFrame(stockhist['date'])
        for i in range(0,len(freqlist)-1):                                     #信号表频率表第N个频率和第N+1频率的信号相减
            shortN=freqlist[i]
            longN=freqlist[i+1]
            ma=getattr(self.VI,signal)(stockhist,longN,shortN)
            clustertab=pd.merge(clustertab,ma,on='date',how='left')
        for i in range(0,len(freqlist)-2):                                     #信号表频率表第N个频率和第N+2频率的信号相减
            shortN=freqlist[i]
            longN=freqlist[i+2]
            ma=getattr(self.VI,signal)(stockhist,longN,shortN)
            clustertab=pd.merge(clustertab,ma,on='date',how='left')
        clustertab=pd.merge(clustertab,stockhist[['date','close']],how='left',on='date')
        clusterZ=clustertab.copy()
        clusterZ.iloc[:,1:-1]=clusterZ.iloc[:,1:-1].div(clusterZ.iloc[:,-1],axis=0)  #Divided by Spot price, to normalize it
        clusterZ=pd.melt(clusterZ,id_vars=['date'],value_vars=clusterZ.columns[1:],var_name='freqdiff',value_name='sigdiff')
        clusterZ=clusterZ.loc[clusterZ['freqdiff']!='close',:].copy()
        return(clusterZ)

    #Given the df with frequencydiff, apply rule-based weighting to frequencies (shortest frequency have highest weights whatsover)
    def Frequency_weight(self,df):
        df['loc']=[x.find('-') for x in df['freqdiff']]
        df['firstfreq']=df.apply(lambda x: x['freqdiff'][0:x['loc']],axis=1)
        df['firstfreq']=[int(x) for x in df['firstfreq']]
        df=df.sort_values(by=['firstfreq'],ascending=[True])
        median=df['firstfreq'].median()
        df['distance']=abs(df['firstfreq']-median)
        df['rank']=df['distance'].rank(method='first',ascending=False)
        perpoint=1/(df['rank'].sum())
        df['weight']=df['rank']*perpoint
        df=df.drop(['firstfreq','distance','rank','loc'], 1)
        return(df)
    
    #Download historical of all stocks, calculate the differences between signals of one freq and a further freq, calculate weighted difference score (equal weight)
    #Return historical price and the calculated score
    def Cross_Cluster(self,tickerlist,rechist):
        signallist=['MA']
        freqlist=[3,10,15,20,60,90,180,360]
        cluster_all=pd.DataFrame()
        newtickerlist=list(rechist['ticker'].unique())
        for ticker in newtickerlist:
            print(ticker)
            stockhist=rechist.loc[rechist['ticker']==ticker,:].copy()
            stockhist=stockhist.loc[stockhist['dailyvolume']>0,:].copy()
            stockhist=stockhist.sort_values(by=['date'],ascending=[True])
            stockhist=stockhist.reset_index(drop=True)
            for signal in signallist:
                clusterZ=self.Clustering(signal,stockhist,freqlist)                #Return the cluster of freqdiffs on rebal days/standardized by spot price
                freqdifflist=list(clusterZ['freqdiff'].unique())
                weight=pd.DataFrame(freqdifflist,columns=['freqdiff'])
                weight=self.Frequency_weight(weight)                               #Mid range frequencies are given the highest weights
                clusterZ=pd.merge(clusterZ,weight[['freqdiff','weight']],how='left',on='freqdiff') 
                clusterZ['weighted']=clusterZ['sigdiff']*clusterZ['weight']          
                clusterZ['score']=clusterZ.groupby(['date'])['weighted'].transform('sum') #weighted sum of standardized sigdiff=>score
                clusterZ=clusterZ.drop_duplicates(subset=['date'])
                clusterZ['signal']=signal
                clusterZ['ticker']=ticker
                clusterZ=clusterZ[['date','score','ticker','signal']]
                cluster_all=cluster_all.append(clusterZ)
        return(cluster_all)
    
    
class Backtest():
    def __init__(self):
        self.PDC=PXDataCollect()
        self.PR=Period_Return()
        self.I=Indicators()
        self.A=Analysis()
        
    #Multiple tickets testing
    def MultiUnderliers(self,tickerlist):
        startdate='2010-01-04'
        rebal_period=40                                                      #How often we assess returns of indicators (500 trading days==2years)
        rechist=self.PDC.Hist_sector_return(tickerlist)                        #Download all stocks basic info
        signallist=['MACD','MA','KDJ','CCI','AROON','DMI','EMV','Chaikin','RSI','TRIX','BOLL','CMO'] 
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)                  #Set the rebaldays given startdate, and rebalance frequency
        topn=3                                                                #choose top3 signals of each period or top N.
        summarydict={}
        for ticker in tickerlist:
            stockhist=rechist.loc[rechist['ticker']==ticker,:].copy()
            stockhist=stockhist.loc[stockhist['volume']>0,:].copy()
            stockhist=stockhist.sort_values(by=['date'],ascending=[True])
            stockhist=stockhist.reset_index(drop=True)
            cumtab,signalperf=self.A.IndicSelect(stockhist,rebaldaylist,signallist,topn)      #Gather indicators performance (after adjustment of HighDD and CumulativeReturn)
            cumtab=self.A.Signal_weight(cumtab)                                                      #Weight Signal according to past 500day period  
            cumtab2=self.A.CombineTopSignals(cumtab,signalperf)                                         #Pick top signals, return cumulative return  
            cumtab2=pd.merge(cumtab2,stockhist[['date','dailyreturn']],on='date',how='left')
            cumtab2['dailyweighted_return']=cumtab2['gen_pos'].shift(1)*cumtab2['dailyreturn']
            cumtab2['dailyweighted_return']=cumtab2['dailyweighted_return'].astype('float')
            cumtab2['cumreturn']=np.exp(np.log1p(cumtab2['dailyweighted_return']).cumsum())   #计算历史cumulative return
            cumtab2=cumtab2[['date','gen_pos','dailyreturn','dailyweighted_return','cumreturn']].copy()
            summarydict[ticker]=cumtab2
        return(summarydict)
        
    
    #Construct the portfolio using clustering signals
    def Cluster_Port(self,targetvol,dailyreturn,switch):
        if switch=='sector':
            rebaldaylist=DC.Rebaldaylist('2010-03-24',18)
        else:
            rebaldaylist=DC.Rebaldaylist('2009-11-30',24)
        rechist,BM_memb=self.PDC.Get_asset_hist(switch,dailyreturn)
        rechistO=rechist.copy()
        rechistO['dailyreturn']=rechistO['dailyreturn'].astype(float)
        #rechist=self.PDC.Get_asset_hist('indexcomponent',dailyreturn)
        tickerlist=list(rechist['ticker'].unique())
        cluster=self.A.Cross_Cluster(tickerlist,rechist)
        universe=DS.Rebalday_alignment(BM_memb,rebaldaylist)
        rebal_cluster=cluster.loc[cluster['date'].isin(rebaldaylist),:].copy()
        rebal_cluster['rebal_mean']=rebal_cluster.groupby(['date'])['score'].transform('mean')
        rebal_cluster['rebal_std']=rebal_cluster.groupby(['date'])['score'].transform('std')
        rebal_cluster['Zscore']=(rebal_cluster['score']-rebal_cluster['rebal_mean'])/rebal_cluster['rebal_std'] #Calculate the Zscore across all stocks on the rebal days
        rebal_cluster['Zscore']=rebal_cluster['Zscore']/3                                                       #Zscore pulled back to [-1,1]
        rechist=rechist.pivot_table(index='date',columns='ticker',values='dailyreturn',aggfunc='first') #make the weight a pivot table 
        rechist=rechist.reset_index(inplace=False)
        rechist=rechist.sort_values(by=['date'],ascending=[True])
        hispos=pd.DataFrame()
        for rebalday in rebaldaylist:
            row=rechist.index[rechist['date']==rebalday][0]
            rebaluniverse=list(universe.loc[universe['date']==rebalday,'ticker'].unique())
            rechist_rebalday=rechist.iloc[row-30:row,1:].copy()            #carve out the last 30trading days before rebalday 
            rebalday_cluster=rebal_cluster.loc[(rebal_cluster['date']==rebalday)&(rebal_cluster['Zscore']>0)&(rebal_cluster['ticker'].isin(rebaluniverse)),:].copy()     #select only score>0 on the rebalday
            rechist_rebalday=rechist_rebalday[rebalday_cluster['ticker']]                                                              #choose the 30day return of stocks with score>0
            baseweight=1/len(rebalday_cluster['ticker'].unique())                 #基本格子宽为0以上的入选股票等权
            weights=rebalday_cluster['Zscore']*baseweight                 #调整格子宽为综合分数*基本格子宽
            port_vol=np.sqrt(np.dot(weights.T,np.dot(rechist_rebalday.cov()*252,weights)))            #Portfolio vol
            adjustment=targetvol/port_vol                                                             #exante vol与targetvol的调整系数
            weights=weights*adjustment                                                                #调整为target vol
            rebalday_cluster['PortNav%']=weights
            hispos=hispos.append(rebalday_cluster)
        hispos['totalweight']=hispos.groupby(['date'])['PortNav%'].transform('sum')
        hispos['shrinkback']=hispos['totalweight']/1
        hispos.loc[hispos['totalweight']>1,'PortNav%']=hispos.loc[hispos['totalweight']>1,'PortNav%']/hispos.loc[hispos['totalweight']>1,'shrinkback']
        if switch=='sector':
            portPNL=RC.DailyPNL(rechistO,hispos)
        else:
            portPNL=RC.DailyPNL(dailyreturn,hispos)
        portPNL['cumPNL']=np.exp(np.log1p(portPNL['dailyreturn']).cumsum())
        return(hispos,portPNL)
        
        
        