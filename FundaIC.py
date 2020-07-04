# -*- coding: utf-8 -*-
"""
Created on Sun May 31 20:44:46 2020

@author: wudi
"""

import pandas as pd
import numpy as np
from FundaStock import Prep as FundaPrep
from FundaStock import Funda as FundaFun
from Toolbox import DataCollect
from Toolbox import DataStructuring

FP=FundaPrep()
FF=FundaFun()
DC=DataCollect()
DS=DataStructuring()

#dailyreturn=DC.Dailyreturn_retrieve()
#siglist=['PE','turnoverweek']
tradingday=pd.read_csv("D:/SecR/Tradingday.csv")


#Return the start and end date of backtest period
def BTdays(tradingday,rebalday,rebal_period):
    dateloc=tradingday.loc[tradingday['date']==rebalday,:].index[0]
    endloc=tradingday.shape[0]
    rebalstart=tradingday.iloc[dateloc+2,0]
    if dateloc+rebal_period+1<=endloc:
        rebalend=tradingday.iloc[dateloc+1+rebal_period,0]
    else:
        rebalend=tradingday['date'].max()
    return(rebalstart,rebalend)
    
    
#Return the cumulative return of stocks in backtest period
def Period_PNL(dailyreturn,sig_rebalday,rebalstart,rebalend):
    periodpnltab=dailyreturn.loc[(dailyreturn['ticker'].isin(sig_rebalday['ticker']))&(dailyreturn['date']>=rebalstart)&(dailyreturn['date']<=rebalend),:]
    periodpnlsum=periodpnltab.groupby(['ticker']).sum()
    periodpnlsum.reset_index(inplace=True)
    periodpnlsum['pnlrank']=periodpnlsum['dailyreturn'].rank(ascending=True)
    return(periodpnlsum)
    

def FundaIC(dailyreturn,startdate,rebal_period,siglist,selectsigs):
    rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
    sighist=FP.SigdataPrep(dailyreturn,startdate,rebal_period,siglist,rebaldaylist)
    sighist=DS.GrowVol(sighist,'grow')
    sighist=sighist.loc[(sighist['sigvalue'].isnull()==False)|(sighist['sigvalue']!=0),:]
    tickerlist=sighist['ticker'].unique().tolist()
    stock_sector=DC.Stock_sector(rebaldaylist,tickerlist)
    Rdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),:].copy()
    Rdailyreturn['mcap']=Rdailyreturn['mcap'].apply(np.log)
    rankIC={}
    for rebalday in rebaldaylist:
        print(rebalday)
        rebalstart,rebalend=BTdays(tradingday,rebalday,rebal_period)
        activestocks_rebalday=dailyreturn.loc[(dailyreturn['date']==rebalday)&(dailyreturn['dailyreturn']!=0),:]['ticker'].unique().tolist() #stocks that are tradign on rebalday
        rebaldatetime=pd.to_datetime(rebalday)
        sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days                                   #current sig is the snapshot of siglist in the last ENDdate prior to rebal day
        current_sig=sighist.loc[(sighist['updatelag']>=-180)&(sighist['updatelag']<=0),:].copy()           #remove the entries where publish dates is more than half a year ago
        current_sig=current_sig.loc[current_sig['ticker'].isin(activestocks_rebalday),:]                   #only stocks tradign on rebalday participate in the analysis
        Mcap_rebalday=Rdailyreturn.loc[(Rdailyreturn['date']==rebalday)&(Rdailyreturn['ticker'].isin(current_sig['ticker'].unique())),:].copy()
        for sig in selectsigs:
            current_onesigWSMcapDummy,Xset=FF.Current_one_signal(sig,current_sig,rebalday,Mcap_rebalday,stock_sector) #Prepare the table with sector dummy and marketcap, to be neutrliazed
            sig_rebalday=DS.Neutralization(current_onesigWSMcapDummy,sig,Xset)
            sig_rebalday['rank']=sig_rebalday['A_'+sig].rank(ascending=True)
            periodpnlsum=Period_PNL(dailyreturn,sig_rebalday,rebalstart,rebalend)
            sig_rebalday=pd.merge(sig_rebalday,periodpnlsum[['ticker','pnlrank']],on='ticker',how='left')
            periodrankIC=sig_rebalday['rank'].corr(sig_rebalday['pnlrank'])
            rankIC[sig+'_'+rebalday]=periodrankIC
        rankIC=pd.DataFrame.from_dict(rankIC)
        rankIC.reset_index(inplace=True)
        rankIC.columns=['Index','RankIC']
    return(rankIC)