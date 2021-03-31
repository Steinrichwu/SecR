# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 21:55:54 2020

@author: wudi
"""
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from HotStock import Prep as HSPrep
from HotStock import Review as HSReview
from HotStock import SecR as SR
from HotStock import StockPick as HSP
from MSSQL import MSSQL
from Toolbox import DataCollect
from Toolbox import WeightScheme
from Toolbox import ReturnCal
from Toolbox import DataStructuring 
from Funda import BTStruct 
from Funda import Prep
from Consensus import Prep as CCP
from HotStock import SecR as SR
import random
import math
from scipy.stats import norm
from sklearn.decomposition import PCA

SP=HSP()
DC=DataCollect()
WS=WeightScheme()
RC=ReturnCal()
DS=DataStructuring()
F=BTStruct()
FP=Prep()
CP=CCP()
HSR=HSReview()
SP=HSP()
FF=BTStruct()
SRDaily=SR()
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

#Merge ranks of ST concensus rank+ PB rank (PB rank is calculated with %-tile PB in the past year自己和自己比)
#先选consensus前6，再用PBtile来决定weights. Weights are mirror weights+ 分级weights depending on rank_PB
def Bankinghotstock2(dailyreturn,rebaldaylist):
    banks=SP.Rec_stat(dailyreturn,rebaldaylist,60,['40'],'CITIC')
    banks2=SP.Rec_stat(dailyreturn,rebaldaylist,250,['40'],'CITIC')
    banks['raccount']=banks['raccount'].astype(int)
    banks2['raccount']=banks2['raccount'].astype(int)
    banks['rank_st']=banks.groupby('date')['raccount'].rank(ascending=True)
    banks2['rank_lt']=banks2.groupby('date')['raccount'].rank(ascending=True)
    banks['index']=banks['date']+banks['ticker']
    banks2['index']=banks2['date']+banks2['ticker']
    banks=pd.merge(banks,banks2[['index','rank_lt']],on='index',how='left')
    PBhist=DC.Stock_valuation_hist(banks)
    PBhist['index']=PBhist['date']+PBhist['ticker']
    banks=pd.merge(banks,PBhist[['index','PB1y-tile']],on='index',how='left')
    banks['rank_PB']=banks.groupby('date')['PB1y-tile'].rank(ascending=False)
    banks['comprank']=banks['rank_st']+banks['rank_PB']
    CSI300hotbanks=WS.Benchmark_intersect(banks,'CSI300')
    CSI300hotbanks=CSI300hotbanks.groupby('date').apply(lambda x: x.nlargest(6,'rank_st')).reset_index(drop=True)
    CSI300hotbanks['rank_PB']=CSI300hotbanks.groupby('date')['PB1y-tile'].rank(ascending=False)
    CSI300hotbanks['PortNav2%']=0.15
    CSI300hotbanks.loc[CSI300hotbanks['rank_PB']>=5,'PortNav2%']=0.25
    CSI300hotbanks.loc[CSI300hotbanks['rank_PB']<=2,'PortNav2%']=0.1
    CSI300hotbanks['totalweight']=CSI300hotbanks.groupby('date')['weight'].transform('sum')
    CSI300hotbanks['PortNav1%']=CSI300hotbanks['weight']/CSI300hotbanks['totalweight']
    CSI300hotbanks['PortNav%']=(CSI300hotbanks['PortNav2%']+CSI300hotbanks['PortNav1%'])/2
    banksPNL=RC.DailyPNL(dailyreturn,CSI300hotbanks)
    banksPNL['CumPNL']=np.exp(np.log1p(banksPNL['dailyreturn']).cumsum())
    return(CSI300hotbanks,banksPNL)


#Lowest PB. Intersect/Weight with CSI300 （PB：横截面，自己和其他比）
def LowPB(dailyreturn,rebaldaylist):
    banks=SP.Rec_stat(dailyreturn,rebaldaylist,60,['40'],'CITIC')
    banks['raccount']=banks['raccount'].astype(int)
    banks['rank_st']=banks.groupby('date')['raccount'].rank(ascending=True)
    banks['index']=banks['date']+banks['ticker']
    PBhist=DC.Bank_PB(banks)
    PBhist['index']=PBhist['date']+PBhist['ticker']
    banks=pd.merge(banks,PBhist[['index','PB']],on='index',how='left')
    banks['PB']=banks['PB'].astype(float)
    banks['rank_PB']=banks.groupby('date')['PB'].rank(ascending=False)
    CSI300hotbanks=WS.Benchmark_intersect(banks,'CSI300')
    CSI300hotbanks=CSI300hotbanks.groupby('date').apply(lambda x: x.nlargest(5,'rank_PB')).reset_index(drop=True)
    CSI300hotbanks['totalweight']=CSI300hotbanks.groupby('date')['weight'].transform('sum')
    CSI300hotbanks['PortNav%']=CSI300hotbanks['weight']/CSI300hotbanks['totalweight']
    banksPNL=RC.DailyPNL(dailyreturn,CSI300hotbanks)
    banksPNL['CumPNL']=np.exp(np.log1p(banksPNL['dailyreturn']).cumsum())
    return(CSI300hotbanks,banksPNL)


#Buy the 5banks trading at the lowest PB range vs the past year (自己和自己比)
def LowPBtile(dailyreturn,rebaldaylist):
    banks=SP.Rec_stat(dailyreturn,rebaldaylist,60,['40'],'CITIC')
    banks['raccount']=banks['raccount'].astype(int)
    banks['rank_st']=banks.groupby('date')['raccount'].rank(ascending=True)
    banks['index']=banks['date']+banks['ticker']
    PBhist=DC.Stock_valuation_hist(banks)
    PBhist['index']=PBhist['date']+PBhist['ticker']
    banks=pd.merge(banks,PBhist[['index','PB1y-tile']],on='index',how='left')
    banks['rank_PB']=banks.groupby('date')['PB1y-tile'].rank(ascending=False)
    CSI300hotbanks=WS.Benchmark_intersect(banks,'CSI300')
    CSI300hotbanks=CSI300hotbanks.groupby('date').apply(lambda x: x.nlargest(5,'rank_PB')).reset_index(drop=True)
    CSI300hotbanks['totalweight']=CSI300hotbanks.groupby('date')['weight'].transform('sum')
    CSI300hotbanks['PortNav%']=CSI300hotbanks['weight']/CSI300hotbanks['totalweight']
    banksPNL=RC.DailyPNL(dailyreturn,CSI300hotbanks)
    banksPNL['CumPNL']=np.exp(np.log1p(banksPNL['dailyreturn']).cumsum())
    return(CSI300hotbanks,banksPNL)
    

#Top5 hotstocks in the last 60 days. Intersect/Weight with CSI300
def Consensus(dailyreturn,rebaldaylist):
    banks=SP.Rec_stat(dailyreturn,rebaldaylist,60,['40'],'CITIC')
    banks['raccount']=banks['raccount'].astype(int)
    banks['rank_st']=banks.groupby('date')['raccount'].rank(ascending=True)
    banks['index']=banks['date']+banks['ticker']
    PBhist=DC.Bank_PB(banks)
    PBhist['index']=PBhist['date']+PBhist['ticker']
    banks=pd.merge(banks,PBhist[['index','PB']],on='index',how='left')
    banks['PB']=banks['PB'].astype(float)
    banks['rank_PB']=banks.groupby('date')['PB'].rank(ascending=False)
    CSI300hotbanks=WS.Benchmark_intersect(banks,'CSI300')
    CSI300hotbanks=CSI300hotbanks.groupby('date').apply(lambda x: x.nlargest(5,'rank_st')).reset_index(drop=True)
    CSI300hotbanks['totalweight']=CSI300hotbanks.groupby('date')['weight'].transform('sum')
    CSI300hotbanks['PortNav%']=CSI300hotbanks['weight']/CSI300hotbanks['totalweight']
    banksPNL=RC.DailyPNL(dailyreturn,CSI300hotbanks)
    banksPNL['CumPNL']=np.exp(np.log1p(banksPNL['dailyreturn']).cumsum())
    return(CSI300hotbanks,banksPNL)


def alignment(df,rebaldaylist):
    daydf=pd.DataFrame(list(df['date'].unique()),columns=['date'])
    rebaldaydf=pd.DataFrame(rebaldaylist,columns=['date'])
    daydf['tocopy']=daydf['date']
    rebaldaydf['tocopy']=np.nan
    daydf=daydf.append(rebaldaydf)
    daydf=daydf.sort_values(by=['date'],ascending=True)
    daydf['tocopy']=daydf['tocopy'].fillna(method='ffill')
    tocopydict=dict(zip(daydf['date'],daydf['tocopy']))
    dfdict=df.groupby('date')['ticker'].apply(list).to_dict()
    newdict={}
    for keys in list(daydf['date'].unique()):
        tocopydate=tocopydict[keys]
        newdict[keys]=dfdict[tocopydate].copy()
    newdf=pd.concat({k:pd.Series(v)for k,v in newdict.items()})
    newdf=newdf.reset_index(inplace=False)
    newdf.columns=(['date','level','ticker'])
    return(newdf)

def Get_secweight(df,publisher):
    df=DC.Sector_get(df,publisher)
    df['index']=df['date']+df['primecode']
    df['secweight']=df.groupby('index')['PortNav%'].transform('sum')
    df=df.drop(['index'],axis=1)
    return(df)

def Sector_constraint(df):
    df['secstock_count']=df.groupby(['index'])['ticker'].transform('count')
    df['secw_cap']=1.1
    df.loc[df['bm_secweight']<=0.1,'secw_cap']=1.3 #1.3
    df.loc[df['bm_secweight']<=0.08,'secw_cap']=1.5 #1.5
    df.loc[df['bm_secweight']<=0.05,'secw_cap']=2 #2
    df.loc[df['bm_secweight']<=0.03,'secw_cap']=2.5 #2.5
    df.loc[df['bm_secweight']<=0.01,'secw_cap']=3 #3
    df['secw_cap']=df['bm_secweight']*df['secw_cap']
    df['deficit']=df['bm_secweight']*0.6-df['secweight']          #deficit: the distance of the lowest sectors to 60% of bm_sec
    df['excess']=df['secweight']-df['secw_cap']                   #excess: the distance of sectors to their caps
    df.loc[df['excess']<=0,'excess']=0
    df.loc[df['deficit']<=0,'deficit']=0
    df['perstockdeficit']=df['deficit']/df['secstock_count']
    df['perstockexcess']=df['excess']/df['secstock_count']
    df['sumdeficit']=df.groupby('date')['perstockdeficit'].transform('sum')
    df['sumexcess']=df.groupby('date')['perstockexcess'].transform('sum')
    df['deficit_proportion']=df['deficit']/df['sumdeficit']       #the % among deficit sectors
    df['toadd_perstock']=df['deficit_proportion']*df['sumexcess']/df['secstock_count']      #which deficit sectors get how much PER STOCK
    df['PortNav%']=df['PortNav%']+df['toadd_perstock']                                      #can add to everyline, the nondeficitone's deficitproportion is 0 anyway
    df['excess_proportion']=df['excess']/df['sumexcess']
    df['added']=df.groupby('date')['toadd_perstock'].transform('sum')
    df['tosubtract_perstock']=df['excess_proportion']*df['added']/df['secstock_count']
    df['PortNav%']=df['PortNav%']-df['tosubtract_perstock']
    df=df[['date','ticker','PortNav%']].copy()
    return(df)
   

def RiskMgmt(df):
    rebaldaylist=list(df['date'].unique())
    membhist=DC.Benchmark_membs('CSI300','2005-01-01')
    benchmark=DS.Rebalday_alignment(membhist,rebaldaylist)
    benchmark=benchmark.rename(columns={'weight':'PortNav%'})
    benchmark['PortNav%']=benchmark['PortNav%']/100
    df=Get_secweight(df,'CITIC')
    benchmark=Get_secweight(benchmark,'CITIC')
    df['index']=df['date']+df['primecode']
    benchmark['index']=benchmark['date']+benchmark['primecode']
    benchmark=benchmark.drop_duplicates(subset='index',keep='first')
    benchmark=benchmark.rename(columns={'secweight':'bm_secweight'})
    df=pd.merge(df,benchmark[['index','bm_secweight']],on='index',how='left')
    #df=Sector_constraint(df)
    return(df)

def Sector_constraint2(df):
    df['index']=df['date']+df['primecode']
    df['lowbar']=0
    df.loc[df['bm_secweight']>=0.05,'lowbar']=df.loc[df['bm_secweight']>=0.05,'bm_secweight']*0.6
    df['deficit']=0
    df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'deficit']=df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'bm_secweight']*0.6-df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'secweight']
    df['secstock_count']=df.groupby(['index'])['ticker'].transform('count')
    df['perstockdeficit']=df['deficit']/df['secstock_count']
    df['sumdeficit']=df.groupby('date')['perstockdeficit'].transform('sum')
    df['perstock_nondeficit']=0
    df.loc[df['perstockdeficit']==0,'perstock_nondeficit']=df.loc[df['perstockdeficit']==0,'PortNav%']
    df['sumnondeficit']=df.groupby('date')['perstock_nondeficit'].transform('sum')
    df['toreduce_percent']=df['sumdeficit']/df['sumnondeficit']
    df.loc[df['perstockdeficit']==0,'PortNav%']=df.loc[df['perstockdeficit']==0,'PortNav%']*(1-df['toreduce_percent'])
    df.loc[df['perstockdeficit']!=0,'PortNav%']=df.loc[df['perstockdeficit']!=0,'PortNav%']+df.loc[df['perstockdeficit']!=0,'perstockdeficit']
    df=df[['date','ticker','PortNav%']].copy()
    return(df)


#universecode='Market'
#universe=P.Universe(dailyreturn,rebaldaylist,universecode)
#sighist=P.SigdataPrep(dailyreturn,['ROETTM','PE','OpRevGrowthYOY'],rebaldaylist)  
def RPG(dailyreturn,rebaldaylist,sighist,universe):
       port=pd.DataFrame(columns=['date','ticker'])
       sighist['index']=sighist['ticker']+sighist['signame']
       sighist['sigvalue']=sighist['sigvalue'].astype(float)
       for rebalday in rebaldaylist:
           print(rebalday)
           rebaldatetime=pd.to_datetime(rebalday)
           sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days    
           rebal_stocklist=list(universe.loc[universe['date']==rebalday,'ticker'].unique())
           rebal_sighist=sighist.loc[(sighist['publdate']<=rebalday)&(sighist['ticker'].isin(rebal_stocklist))&(sighist['updatelag']>=-180),:].copy()
           rebal_sighist=rebal_sighist.drop_duplicates('index','last')
           ROE=rebal_sighist.loc[rebal_sighist['signame']=='ROETTM',:].copy()
           stockcount=len(ROE['ticker'].unique())
           ROEselect=ROE.nlargest(int(stockcount*0.2),'sigvalue',keep='all')
           ROEselectticker=list(ROEselect.loc[ROEselect['sigvalue']>0,'ticker'].unique())
           stockcount=len(ROEselectticker)
           Value=rebal_sighist.loc[(rebal_sighist['signame']=='PE')&(rebal_sighist['ticker'].isin(ROEselectticker)),:].copy()
           Valueselect=Value.nlargest(int(stockcount*0.2),'sigvalue',keep='all')
           selectdf=pd.DataFrame(data={'date':rebalday,'ticker':Valueselect['ticker']})
           port=port.append(selectdf)
       port['memb']=port.groupby('date')['ticker'].transform('count')           #Take intersect, but use equal weight
       port['PortNav%']=1/port['memb']
       port=port.drop(['memb'],axis=1)
       #IntersecPort=BST.Intersect_weighting(port,'CSI800')
       #PNL=BT.GenPNL(IntersecPort,dailyreturn)
       #PNLreview=PC.Alpha(PNL)
       return(port)

def FNtab(fzdict):
        itemlist=fzdict.keys()
        facnamelist=list(set([x.split('_')[0]for x in itemlist]))
        rebaldaylist=list(set([x.split('_')[1]for x in itemlist]))
        rebaldaylist.sort()
        colnames=['date','ticker']+[x+'_zscore'for x in facnamelist]
        fztab=pd.DataFrame(columns=colnames)
        for rebalday in rebaldaylist:
            rebalztab=pd.DataFrame()
            for facname in facnamelist:
                ztab=fzdict[facname+'_'+rebalday].copy()
                ztab=ztab.drop_duplicates()
                if rebalztab.shape[0]==0:
                   rebalztab=ztab.copy()
                else:
                   rebalztab=pd.merge(rebalztab,ztab,on='ticker',how='outer')
            rebalztab.insert(0,column='date',value=rebalday)            
            fztab=fztab.append(rebalztab)
        #fztab=fztab.drop('index', 1)
        return(fztab)

def FactorDeviation(rebaldaylist,facdict,dailyreturn):
    metricname=facdict[list(facdict.keys())[0]][0]
    fzdict=F.Fzdict(dailyreturn,rebaldaylist,facdict,'Market')                                                             
    fntab=FNtab(fzdict)
    fntab['median_value']=fntab.groupby(['date','Q'])['N_'+metricname].transform('median')           #Take intersect, but use equal weight
    statstab=fntab[['date','Q','median_value']].copy()
    statstab=statstab.drop_duplicates()
    statstab=statstab.pivot_table(index='date',columns='Q',values='median_value',aggfunc='first')
    statstab=statstab.reset_index(inplace=False)
    statstab['diff']=statstab[5]-statstab[1]
    statstab['diff']=statstab['diff'].abs()
    statstab['rollstdev']= statstab['diff'].rolling(20).std()
    statstab['rollmean'] = statstab['diff'].rolling(20).mean()
    statstab['dispersion']=(statstab['diff']-statstab['rollmean'])/statstab['rollstdev']
    return(statstab)


def Factorcrowdedness(rebaldaylist,facdict,turnover3m,dailyreturn):
    metricname=facdict[list(facdict.keys())[0]][0]
    fzdict=F.Fzdict(dailyreturn,rebaldaylist,facdict,'Market')                                                             
    fntab=FNtab(fzdict)
    rdailyreturn=turnover3m.loc[dailyreturn['date'].isin(fntab['date']),:].copy()
    rdailyreturn['index']=rdailyreturn['date']+rdailyreturn['ticker']
    fntab['index']=fntab['date']+fntab['ticker']
    fntab=pd.merge(fntab,rdailyreturn[['index','turnoverrate3m']],on='index',how='left')
    fntab['Q_median_turnover']=fntab.groupby(['date','Q'])['turnoverrate3m'].transform('median')
    fntab['Q_median_turnover']=fntab['Q_median_turnover'].rolling(5).mean()
    statstab=fntab[['date','Q','Q_median_turnover']].copy()
    statstab=statstab.drop_duplicates()
    statstab=statstab.pivot_table(index='date',columns='Q',values='Q_median_turnover',aggfunc='first')
    statstab=statstab.reset_index(inplace=False)
    statstab['diff']=statstab[5]-statstab[1]
    statstab['diff']=statstab['diff'].abs()
    statstab['rollstdev']= statstab['diff'].rolling(20).std()
    statstab['rollmean'] = statstab['diff'].rolling(20).mean()
    statstab['dispersion']=(statstab['diff']-statstab['rollmean'])/statstab['rollstdev']
    return(statstab)

def ValuationBand(df):
    daylist=list(df['date'].unique())
    valuedf=FP.ValuationReciprocal_download(daylist,'PE')
    df['index']=df['date']+df['ticker']
    valuedf['publdate']=valuedf['publdate'].astype(str)
    valuedf['index']=valuedf['publdate']+valuedf['ticker']
    df=pd.merge(df,valuedf[['index','sigvalue']],on='index',how='left')
    df=DS.Winsorize(df,'sigvalue',0.05)              #稍带后视镜的winsorization 
    df['sigvalue']=1/df['sigvalue']
    df['weightedPE']=df['PortNav%']*df['sigvalue']
    dfweightedPE=df.groupby(['date'])['weightedPE'].sum()
    dfweightedPE=dfweightedPE.reset_index(inplace=False)
    dfweightedPE.columns=['date','weightedPE']
    dfweightedPE['sigma']=dfweightedPE['weightedPE'].rolling(24).std()
    dfweightedPE['mean']=dfweightedPE['weightedPE'].rolling(24).mean()
    dfweightedPE['upperband']=dfweightedPE['mean']+dfweightedPE['sigma']
    dfweightedPE['lowerband']=dfweightedPE['mean']-dfweightedPE['sigma']
    dfweightedPE=dfweightedPE.drop(['sigma','mean'],axis=1)
    return(dfweightedPE)

def EstPEGband(rebaldaylist):
    df=DC.Benchmark_membs('CSI300','2008-12-28')
    df=DS.Rebalday_alignment(df,rebaldaylist)
    daylist=list(df['date'].unique())
    selectdf=CP.DataExtract(daylist,'con_peg')
    df['PortNav%']=df['weight']/100
    df['index']=df['date']+df['ticker']
    selectdf['index']=selectdf['date']+selectdf['ticker']
    df=pd.merge(df,selectdf[['index','con_peg']],on='index',how='left')
    medianPEG=df.groupby(['date'])['con_peg'].median()
    medianPEG=medianPEG.reset_index(inplace=False)
    medianPEG['sigma']=medianPEG['con_peg'].rolling(24).std()
    medianPEG['mean']=medianPEG['con_peg'].rolling(24).mean()
    medianPEG['upperband']=medianPEG['mean']+medianPEG['sigma']
    medianPEG['lowerband']=medianPEG['mean']-medianPEG['sigma']
    medianPEG['2xupperband']=medianPEG['mean']+medianPEG['sigma']*2
    medianPEG['2xlowerband']=medianPEG['mean']-medianPEG['sigma']*2
    medianPEG=medianPEG.drop(['sigma','mean'],axis=1)
    return(medianPEG)

def ROEMedian_Cal(rebaldaylist,dailyreturn):
    tradingdays=list(dailyreturn['date'].unique())
    tradingdays=[pd.to_datetime(x) for x in tradingdays]
    newrebaldaylist=[]
    for rebalday in rebaldaylist:
       rebaldatetime=pd.to_datetime(rebalday)
       newrebalday=min(tradingdays,key=lambda x: abs(x-rebaldatetime))
       newrebalday=newrebalday.strftime("%Y-%m-%d")
       newrebaldaylist.append(newrebalday)
    universecode='Market'
    universe=FP.Universe(dailyreturn,newrebaldaylist,universecode)
    sighist=FP.SigdataPrep(dailyreturn,['ROETTM'],newrebaldaylist)  
    sighist['index']=sighist['ticker']+sighist['signame']
    sighist['sigvalue']=sighist['sigvalue'].astype(float)
    ROEmedianlist=[]
    for newrebalday in newrebaldaylist:
       print(newrebalday)
       rebaldatetime=pd.to_datetime(newrebalday)
       sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days    
       rebal_stocklist=list(universe.loc[universe['date']==newrebalday,'ticker'].unique())
       rebal_sighist=sighist.loc[(sighist['publdate']<=newrebalday)&(sighist['ticker'].isin(rebal_stocklist))&(sighist['updatelag']>=-180),:].copy()
       rebal_sighist=rebal_sighist.drop_duplicates('index','last')
       ROE=rebal_sighist.loc[rebal_sighist['signame']=='ROETTM',:].copy()
       ROEmedianlist.append(ROE['sigvalue'].median())
    ROEmedian=pd.DataFrame({'date':rebaldaylist,'ROEmeidan':ROEmedianlist})
    return(ROEmedian)

def Sector_constraint2(df):
       df['index']=df['date']+df['primecode']
       df['lowbar']=0
       df.loc[df['bm_secweight']>=0.05,'lowbar']=df.loc[df['bm_secweight']>=0.05,'bm_secweight']*0.6
       df['deficit']=0
       df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'deficit']=df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'bm_secweight']*0.6-df.loc[(df['lowbar']>0)&(df['secweight']<df['lowbar']),'secweight']
       df['secstock_count']=df.groupby(['index'])['ticker'].transform('count')
       df['perstockdeficit']=df['deficit']/df['secstock_count']
       df['sumdeficit']=df.groupby('date')['perstockdeficit'].transform('sum')
       df['perstock_nondeficit']=0
       df.loc[df['perstockdeficit']==0,'perstock_nondeficit']=df.loc[df['perstockdeficit']==0,'PortNav%']
       df['sumnondeficit']=df.groupby('date')['perstock_nondeficit'].transform('sum')
       df['toreduce_percent']=df['sumdeficit']/df['sumnondeficit']
       df.loc[df['perstockdeficit']==0,'PortNav%']=df.loc[df['perstockdeficit']==0,'PortNav%']*(1-df['toreduce_percent'])
       df.loc[df['perstockdeficit']!=0,'PortNav%']=df.loc[df['perstockdeficit']!=0,'PortNav%']+df.loc[df['perstockdeficit']!=0,'perstockdeficit']
       df=df[['date','ticker','PortNav%','secweight','bm_secweight','primecode']].copy()
       return(df)

def BrokerSector_overweight(df):
    df['brokernot']=0
    df.loc[df['primecode']=='4110','brokernot']=1
    df['brokercount']=df.groupby(['date'])['brokernot'].transform('sum')
    df['brokernot']=df['brokernot'].astype(str)
    df['index']=df['date']+df['brokernot']
    df['count']=df.groupby(['date','index'])['ticker'].transform('count')
    df['brokersecdiff']=0
    df.loc[df['primecode']=='4110','brokersecdiff']=df.loc[df['primecode']=='4110','bm_secweight']-df.loc[df['primecode']=='4110','secweight']
    df['sumtoallocate']=df.groupby(['date'])['brokersecdiff'].transform('sum')
    df['sumtoallocate']=df['sumtoallocate']/df['brokercount']+0.01
    df['perstockallocate']=df['sumtoallocate']/df['count']
    df.loc[df['brokernot']=='0','perstockallocate']=df.loc[df['brokernot']=='0','perstockallocate']*(-1)
    df['PortNav%']=df['PortNav%']+df['perstockallocate']
    df=df.drop(['brokernot', 'brokercount', 'index', 'count', 'brokersecdiff','sumtoallocate', 'perstockallocate'],axis=1)
    df['secweight']=df.groupby(['date','primecode'])['PortNav%'].transform('sum')
    return(df)


def Bank_overweight(df):
    df['brokernot']=0
    df.loc[df['primecode']=='40','brokernot']=1
    df['brokercount']=df.groupby(['date'])['brokernot'].transform('sum')
    df['brokernot']=df['brokernot'].astype(str)
    df['index']=df['date']+df['brokernot']
    df['count']=df.groupby(['date','index'])['ticker'].transform('count')
    df['brokersecdiff']=0
    df.loc[df['primecode']=='40','brokersecdiff']=df.loc[df['primecode']=='40','bm_secweight']-df.loc[df['primecode']=='40','secweight']
    df['sumtoallocate']=df.groupby(['date'])['brokersecdiff'].transform('sum')
    df['sumtoallocate']=df['sumtoallocate']/df['brokercount']
    df['perstockallocate']=df['sumtoallocate']/df['count']
    df.loc[df['brokernot']=='0','perstockallocate']=df.loc[df['brokernot']=='0','perstockallocate']*(-1)
    df['PortNav%']=df['PortNav%']+df['perstockallocate']
    df=df.drop(['brokernot', 'brokercount', 'index', 'count', 'brokersecdiff','sumtoallocate', 'perstockallocate'],axis=1)
    df['secweight']=df.groupby(['date','primecode'])['PortNav%'].transform('sum')
    return(df)

def AddSignaltoPos(dailyreturn,mp):
    DR=pd.read_csv("D:/SecR/DiscountRate.csv")
    rebaldaylist=list(DR['date'].unique())
    tradingdays=list(dailyreturn['date'].unique())
    tradingdays=[pd.to_datetime(x) for x in tradingdays]
    newrebaldaylist=[]
    for rebalday in rebaldaylist:
        rebaldatetime=pd.to_datetime(rebalday)
        newrebalday=min(tradingdays,key=lambda x: abs(x-rebaldatetime))
        newrebalday=newrebalday.strftime("%Y-%m-%d")
        newrebaldaylist.append(newrebalday)
    rebaldaylist=list(mp['date'].unique())
    NDR=DS.Rebalday_alignment(DR,rebaldaylist)
    mp=WS.MixedSecWeight(mp)                #download the new primecode and sector weight (using mixed sector 银行保险券商分开)
    mp=mp.rename(columns={'secweight':'bm_secweight'})
    mp['secweight']=mp.groupby(['date','primecode'])['PortNav%'].transform('sum')
    mp=pd.merge(mp,NDR,on='date',how='left')
    newmp=pd.DataFrame()
    for rebalday in rebaldaylist:
        print(rebalday)
        newdf=mp.loc[mp['date']==rebalday,:].copy()
        newdf=newdf.reset_index(drop=True)
        if (newdf['signal'][0]==1):
            newdf=Bank_overweight(newdf)
        #if (newdf['signal'][0]==-1):
        #   newdf=BrokerSector_overweight(newdf)
        newmp=newmp.append(newdf)
    return(newmp)

def EStAccuracy():
    EPSestimates=pd.read_csv("D:/SecR/EPS_Estimates.csv")
    EPShistory=pd.read_csv("D:/SecR/EPS_history.csv")
    datedf=pd.DataFrame(pd.date_range(start='2008-12-31',end='2020-09-30',freq='D').tolist(),columns=['date'])
    datedf['date']=datedf['date'].astype(str)
    EPShistory=pd.merge(datedf,EPShistory,on='date',how='left')
    EPShistory=EPShistory.fillna(method='ffill')
    newEst=pd.melt(EPSestimates,id_vars=['date'],value_vars=list(EPSestimates.columns[1:]),var_name='ticker',value_name='estEPS')
    newHist=pd.melt(EPShistory,id_vars=['date'],value_vars=list(EPShistory.columns[1:]),var_name='ticker',value_name='histEPS')
    newEst['year']=newEst['date'].str[0:4].astype(int)
    newEst['year']=newEst['year']+1
    newEst['month']=newEst['date'].str[5:]
    newEst['year']=newEst['year'].astype(str)
    newEst['oneyearlater']=newEst['year']+'-'+newEst['month']
    newHist=newHist.rename(columns={'date':'oneyearlater'})
    newHist['index']=newHist['oneyearlater']+newHist['ticker']
    newEst['index']=newEst['oneyearlater']+newEst['ticker']
    newEst=pd.merge(newEst,newHist[['index','histEPS']],on='index',how='left')
    newEst['OverEst']=newEst['estEPS']/newEst['histEPS']-1
    OverEst=newEst.groupby('date')['OverEst'].median()
    OverEst=OverEst.reset_index(inplace=False)
    return(newEst,OverEst)

def ForwardPE():
    FPE=pd.read_csv("D:/SecR/ForwardPE_history.csv")
    FPEtab=pd.melt(FPE,id_vars=['date'],value_vars=list(FPE.columns[1:]),var_name='ticker',value_name='FPE')
    FPEtab['ticker']=[str(x)[0:6]for x in FPEtab['ticker']]
    FPEtab=FPEtab.loc[FPEtab['FPE'].isnull()==False,:].copy()
    FPEtab=DC.Sector_get(FPEtab,'CSI')
    MarketFPE_median=FPEtab.groupby(['date'])['FPE'].median()
    TechFPE_median=FPEtab.loc[FPEtab['primecode']=='04',:].groupby(['date'])['FPE'].median()
    MarketFPE_median=MarketFPE_median.reset_index(inplace=False)
    TechFPE_median=TechFPE_median.reset_index(inplace=False)
    MarketFPE_median.to_csv("D:/MISC/Market.csv",index=False)
    TechFPE_median.to_csv("D:/MISC/Consumer.csv",index=False)
    return()

def tile_PE():
    siglist=['PE']
    sighist=P.SigdataPrep(dailyreturn,siglist,rebaldaylist)
    PE=sighist.copy()
    PE=PE[['publdate','ticker','sigvalue']]
    PE=PE.loc[PE['sigvalue']>=0,:].copy()
    PE=PE.rename(columns={'publdate':'date'})
    PE['date']=[str(x)[0:10] for x in PE['date']]
    PE=DC.Sector_get(PE,'CSI')
    PE=PE.loc[~PE['primecode'].isin(['06']),:].copy()
    PE['25tile']=PE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.25))
    PE['75tile']=PE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.75))
    PE['50tile']=PE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.50))
    PE['divergence']=PE['75tile']/PE['25tile']
    #PE.to_csv("D:/MISC/PEdivergence.csv",index=False)
    TechPE=PE.loc[PE['primecode']=='07',:].copy()
    TechPE['Tech50tile']=TechPE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.75))
    HealthPE=PE.loc[PE['primecode']=='05',:].copy()
    HealthPE['Health50tile']=HealthPE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.75))
    FoodPE=PE.loc[PE['primecode']=='04',:].copy()
    FoodPE['Food50tile']=FoodPE.groupby(['date'])['sigvalue'].transform(lambda x: x.quantile(0.75))
    HealthPE=HealthPE.drop_duplicates(subset='date',keep='first')
    FoodPE=FoodPE.drop_duplicates(subset='date',keep='first')
    TechPE=TechPE.drop_duplicates(subset='date',keep='first')
    PE=PE.drop_duplicates(subset='date',keep='first')
    PE=PE.merge(FoodPE[['date','Food50tile']],on='date',how='left')
    PE=PE.merge(TechPE[['date','Tech50tile']],on='date',how='left')
    PE=PE.merge(HealthPE[['date','Health50tile']],on='date',how='left')
    PE.to_csv("D:/MISC/PEdivergence.csv",index=False)
    return()

#2020/10/29 (30)： Test the performance of ratings/simple risk parity/equal weight.
def Chaojiqushi_weight(dailyreturn):
    CJQS=pd.read_csv("D:/MISC/CJQS_Composite.csv")
    CJQS['ticker']=[str(x)[0:6]for x in CJQS['ticker']]
    rebaldaylist=DC.Rebaldaylist('2019-12-31',5)
    siglist=['PE']
    sighist=FP.SigdataPrep(dailyreturn,siglist,rebaldaylist)
    sighist=sighist[['publdate','ticker','sigvalue']]
    histPE=sighist.loc[(sighist['ticker'].isin(CJQS['ticker']))&(sighist['publdate'].isin(rebaldaylist)),:].copy()
    histPE=histPE.rename(columns={'publdate':'date'})
    histPE['date']=[str(x)[0:10] for x in histPE['date']]
    selectdf=CP.DataExtract(rebaldaylist,'con_np_yoy')
    selectdf=selectdf.loc[selectdf['targetyear']==2021,:].copy()
    selectdf['index']=selectdf['date']+selectdf['ticker']
    histPE['index']=histPE['date']+histPE['ticker']
    histPE=pd.merge(histPE,selectdf[['index','con_np_yoy']],on='index',how='left')
    histPE['sigvalue']=1/histPE['sigvalue']
    histPE['con_np_yoy']=histPE.groupby(['ticker'])['con_np_yoy'].fillna(method='ffill')
    histPE=histPE.dropna()
    histPE['Grow_rank']=histPE.groupby('date')['con_np_yoy'].rank()
    histPE['PE_rank']=histPE.groupby('date')['sigvalue'].rank(ascending=False)
    histPE['pointofstock']=histPE['Grow_rank']+histPE['PE_rank']
    histPE['totalPoint']=histPE.groupby(['date'])['pointofstock'].transform('sum')
    histPE['PortNav%']=histPE['pointofstock']/histPE['totalPoint']
    PNLRating=RC.DailyPNL(dailyreturn,histPE)
    histPE2=histPE.copy()
    histPE2['PortNav%']=histPE.groupby(['date'])['ticker'].transform('count')
    histPE2['PortNav%']=1/histPE2['PortNav%']
    PNLEqual=RC.DailyPNL(dailyreturn,histPE2)
    #calculate the point in time volatility:
    pricetab=dailyreturn.loc[(dailyreturn['ticker'].isin(histPE['ticker']))&(dailyreturn['date']>='2019-10-01'),['date','ticker','dailyreturn']].copy()
    pricetab['30dvol']=pricetab.groupby(['ticker'])['dailyreturn'].transform(lambda x: x.rolling(30).std())
    pricetab['30dvol']=pricetab['30dvol']*16
    histPE3=histPE.copy()
    pricetab['index']=pricetab['date']+pricetab['ticker']
    histPE3['index']=histPE3['date']+histPE3['ticker']
    histPE3=pd.merge(histPE3,pricetab[['index','30dvol']],on='index',how='left')
    histPE3['reciprocal_vol']=1/histPE3['30dvol']
    histPE3['day_total_vol_reci']=histPE3.groupby(['date'])['reciprocal_vol'].transform('sum')
    histPE3['PortNav%']=histPE3['reciprocal_vol']/histPE3['day_total_vol_reci']
    PNLRP=RC.DailyPNL(dailyreturn,histPE3)
    PNLEqual=PNLEqual.rename(columns={'dailyreturn':'PNLEqual'})
    PNLRP=PNLRP.rename(columns={'dailyreturn':'PNLRP'})
    PNLRating=PNLRating.rename(columns={'dailyreturn':'PNLRating'})
    PNLEqual=pd.merge(PNLEqual,PNLRP,on='date',how='left')
    PNLEqual=pd.merge(PNLEqual,PNLRating,on='date',how='left')
    return(PNLEqual)

def Turnovercal(df):
    stocklist=list(df['ticker'].unique())
    rebaldaylist=list(df['date'].unique())
    rebaldaytab=[]
    for rebalday in rebaldaylist:
        rebaldaypos=pd.DataFrame({'ticker':stocklist})
        rebaldaypos['date']=rebalday
        rebaldaytab.append(rebaldaypos)
    rebaldaytab=pd.concat(rebaldaytab)
    df['index']=df['date']+df['ticker']
    rebaldaytab['index']=rebaldaytab['date']+rebaldaytab['ticker']
    rebaldaytab=pd.merge(rebaldaytab,df[['index','PortNav%']],on='index',how='left')
    rebaldaytab['PortNav%']=rebaldaytab['PortNav%'].fillna(0)
    rebaldaytab=rebaldaytab.sort_values(by=['ticker','date'],ascending=[True,True]) 
    rebaldaytab['poschange']=rebaldaytab.groupby(['ticker'])['PortNav%'].transform(lambda x: x-x.shift(1))
    rebaldaytab.loc[rebaldaytab['poschange'].isna()==True,'poschange']=rebaldaytab.loc[rebaldaytab['poschange'].isna()==True,'PortNav%']
    rebaldaytab['poschange']=rebaldaytab['poschange'].abs()
    turnoverstats=rebaldaytab.groupby(['date'])['poschange'].sum()
    turnoverstats=turnoverstats.reset_index(inplace=False)
    return(turnoverstats)

#Intersect_weights
def Intersect_weighting(df,benchmark):
        dfsect=WS.Benchmark_intersect(df,benchmark)
        dfsect['weight']=dfsect['weight']/100
        dfsect['totalweight%']=dfsect.groupby('date')['weight'].transform('sum')
        dfsect['PortNav%']=dfsect['weight']/dfsect['totalweight%']
        dfsect=dfsect[['date','ticker','PortNav%']]
        return(dfsect)

#Compare the 60day recs with longterm 60day mean rac, choose the top 50 with the highest increase. 
def HotStock_Transit(rebaldaylist,dailyreturn):
    activepickhist=SP.Rec_stat(dailyreturn,rebaldaylist,40,'N','CITIC')
    activepickhist['rac_rank']=activepickhist.groupby(['date'])['raccount'].rank("min",ascending=False)
    activepickhist=activepickhist.loc[activepickhist['rac_rank']<=150,:].copy()
    activepickhist=activepickhist.sort_values(by=['ticker','date'],ascending=[True,True])
    activepickhist['rollavgrec']=activepickhist.groupby(['ticker'])['raccount'].transform(lambda x: x.rolling(10).mean())
    activepickhist['rec_chg']=activepickhist['raccount']/activepickhist['rollavgrec']
    activepickhist['rec_chg_rank']=activepickhist.groupby(['date'])['rec_chg'].rank("min", ascending=False)
    newhotstock=activepickhist.loc[activepickhist['rec_chg_rank']<=60,:].copy()
    #newhotstock=Intersect_weighting(newhotstock,'CSI300')
    newhotstock['count']=newhotstock.groupby(['date'])['ticker'].transform('count')
    newhotstock['PortNav%']=1/newhotstock['count']
    PNL=RC.DailyPNL(dailyreturn,newhotstock)
    PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
    return(PNL,newhotstock)

def HotStock_Transit_Composite(rebaldaylist,dailyreturn):
    activepickhist=SP.Rec_stat(dailyreturn,rebaldaylist,30,'N','CITIC')
    activepickhist=activepickhist.sort_values(by=['ticker','date'],ascending=[True,True])
    activepickhist['rollexpand']=activepickhist.groupby(['ticker'])['raccount'].transform(lambda x: x.expanding().mean())
    activepickhist['rollrecentavg']=activepickhist.groupby(['ticker'])['raccount'].transform(lambda x: x.rolling(10).mean())
    activepickhist.loc[activepickhist['rollrecentavg'].isnull()==True,'rollrecentavg']=activepickhist.loc[activepickhist['rollrecentavg'].isnull()==True,'rollexpand']
    activepickhist['rec_chg']=activepickhist['raccount']-activepickhist['rollrecentavg']
    activepickhist['rec_chg_rank']=activepickhist.groupby(['date'])['rec_chg'].rank("min", ascending=False)
    newhotstock=activepickhist.loc[activepickhist['rec_chg_rank']<=60,:].copy()
    newhotstock=Intersect_weighting(newhotstock,'CSI300')
    #newhotstock['count']=newhotstock.groupby(['date'])['ticker'].transform('count')
    #newhotstock['PortNav%']=1/newhotstock['count']
    PNL=RC.DailyPNL(dailyreturn,newhotstock)
    PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
    return(PNL,newhotstock)


def HotStock(rebaldaylist,dailyreturn):
    activepickhist=SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CITIC')
    activepickhist2=SP.Rec_stat(dailyreturn,rebaldaylist,5,'N','CITIC')
    activepickhist2=activepickhist2.rename(columns={'raccount':'ST_raccount'})
    activepickhist['index']=activepickhist['date']+activepickhist['ticker']
    activepickhist2['index']=activepickhist2['date']+activepickhist2['ticker']
    activepickhist=pd.merge(activepickhist,activepickhist2[['index','ST_raccount']],on='index',how='left')
    activepickhist=activepickhist.fillna(0)
    activepickhist['raccount']=activepickhist['raccount']-activepickhist['ST_raccount']
    activepickhist['rec_rank']=activepickhist.groupby(['date'])['raccount'].rank("min", ascending=False)
    hotstock=activepickhist.loc[activepickhist['rec_rank']<=50,:].copy()
    hotstock=Intersect_weighting(hotstock,'CSI300')
    #hotstock['count']=hotstock.groupby(['date'])['ticker'].transform('count')
    #hotstock['PortNav%']=1/hotstock['count']
    PNL=RC.DailyPNL(dailyreturn,hotstock)
    PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
    return(PNL,hotstock)

def ProduceQ1toQ5(universe,rebaldaylist,dailyreturn):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
        selectsigs=[]
        [selectsigs.extend(v) for k, v in facdict.items()]
        siglist=list(set([x.replace('growth','') for x in selectsigs]))
        siglist=list(set([x.replace('vol','') for x in siglist]))
        sighist=FP.SigdataPrep(dailyreturn,siglist,rebaldaylist)     
        sighist=DS.GrowVol(sighist,'grow')
        nsigdict=FF.NSighist(dailyreturn,rebaldaylist,sighist,selectsigs,universe)
        facnamelist=facdict.keys()
        facinfacz=[x+'_zscore' for x in facnamelist]
        fzdict={}
        for facname in facnamelist:
            siginfac=facdict[facname]
            fzdict=FF.Factorscore(rebaldaylist,nsigdict,facname,siginfac,fzdict)            
        fztab=FF.FZtab(fzdict)
        fztab['narow']=fztab.isnull().sum(axis=1)
        fztab['meanscore']=np.mean(fztab[facinfacz],axis=1)
        fztab['rank']=fztab.groupby(['date'])['meanscore'].transform(lambda x: pd.qcut(x.values,5,labels=[1,2,3,4,5]))
        return(fztab)

def Generate_PortNavMcap_CAP(df2,dailyreturn):
        df2['toAllocate']=df2['PortNav%']-0.15
        df2['toAllocate']=df2.loc[df2['PortNav%']>0.15].groupby(['date'])['toAllocate'].transform('sum')
        df2=df2.sort_values(by=['date','toAllocate'])
        df2['toAllocate']=df2['toAllocate'].fillna(method='ffill')
        df2['toAllocate']=df2['toAllocate'].fillna(0)
        df2['allocate%']=df2.loc[df2['PortNav%']<0.15].groupby(['date'])['PortNav%'].transform('sum')
        df2['allocate%']=df2['PortNav%']/df2['allocate%']    
        df2['toAdd']=df2['toAllocate']*df2['allocate%']
        df2['PortNav%']=df2['PortNav%']+df2['toAdd']
        df2.loc[df2['PortNav%'].isna()==True,'PortNav%']=0.15
        df2=df2[['date','ticker','PortNav%']]
        return(df2)

def NewSSZ(rebaldaylist,dailyreturn):
     universe=pd.read_csv("D:/SecR/NewThreeFour_20200802.csv") 
     universe['ticker']=[str(x).zfill(6)for x in universe['ticker']]
     universe=DS.Rebalday_alignment(universe,rebaldaylist)
     universe['index']=universe['date']+universe['ticker']
     mcaptab=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist)),['date','ticker','mcap']].copy()
     mcaptab['index']=mcaptab['date']+mcaptab['ticker']
     mcaptab['bottom30']=mcaptab.groupby(['date'])['mcap'].transform(lambda x: x.quantile(0.4))
     mcaptab['diff']=mcaptab['mcap']-mcaptab['bottom30']
     mcaptab=mcaptab.loc[mcaptab['diff']>=0,:].copy()
     mcaptab=mcaptab.loc[mcaptab['index'].isin(universe['index']),:].copy()
     universe=mcaptab[['date','ticker','mcap']].copy()
     universe=DC.Sector_get(universe,'CITIC')
     fztab=ProduceQ1toQ5(universe,rebaldaylist,dailyreturn)
     fztab=fztab.loc[fztab['rank']>=3,:].copy()
     fztab['index']=fztab['date']+fztab['ticker']
     mcaptab=mcaptab.loc[mcaptab['index'].isin(fztab['index']),:]
     mcaptab['totalMcap']=mcaptab.groupby(['date'])['mcap'].transform('sum')
     mcaptab['PortNav%']=mcaptab['mcap']/mcaptab['totalMcap']
     while (mcaptab['PortNav%'].max()>0.15):
            mcaptab=Generate_PortNavMcap_CAP(mcaptab,dailyreturn)
     PNL=RC.DailyPNL(dailyreturn,mcaptab)
     PNL16=PNL.loc[PNL['date']>='2016-01-01',:].copy()
     PNL20=PNL.loc[PNL['date']>='2020-01-01',:].copy()
     PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
     PNL16['cumReturn']=np.exp(np.log1p(PNL16['dailyreturn']).cumsum())
     PNL20['cumReturn']=np.exp(np.log1p(PNL20['dailyreturn']).cumsum())
     return(PNL,PNL16,PNL20,mcaptab)

def Return_attribution():
    indexreturn=pd.read_csv("D:/MISC/index.csv")
    facreturn=pd.read_csv("D:/MISC/facreturnhist.csv")
    facreturn=facreturn.loc[facreturn['date'].isin(indexreturn['date']),:].copy()
    indexreturn=indexreturn.loc[indexreturn['date'].isin(facreturn['date']),:].copy()
    x=facreturn.iloc[:,1:].copy()
    y=indexreturn.iloc[:,1:].copy()
    beta=np.linalg.inv(x.T.dot(x)).dot(x.T).dot(y)
    beta=pd.DataFrame(beta,columns=['beta'])
    return(beta)

    

#Return the dailyreturn of every CITIC sector's top five marketcap portfolio and the top 5 stocks of every sector (mean of three)
def CITICsectop_return(dailyreturn,rebaldaylist):
    mcap=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist)),['date','ticker','mcap']].copy()
    mcap_sec=DC.Sector_get(mcap,'CITIC')
    mcap_sec['index']=mcap_sec['date']+mcap_sec['primecode']
    mcapTop=mcap_sec.groupby('index').apply(lambda x: x.nlargest(5,'mcap')).reset_index(drop=True)
    mcapTop2=mcapTop.pivot_table(index='date',columns='ticker',values='mcap',aggfunc='first')
    mcapTop2.reset_index(inplace=True)
    mcapTop2=mcapTop2.fillna(0)
    tradingday=pd.DataFrame(list(dailyreturn.loc[dailyreturn['date']>='2010-01-01','date'].unique()),columns=['date'])
    mcapTop2=pd.merge(tradingday,mcapTop2,on='date',how='left')
    mcapTop2=mcapTop2.fillna(method='ffill')
    mcapTop2=pd.melt(mcapTop2,id_vars='date',value_vars=list(mcapTop2.columns[1:len(mcapTop2.columns)+1]),var_name='Ticker',value_name='mcap')    
    mcapTop2=mcapTop2.loc[mcapTop2['mcap']>0,:].copy()
    mcapTop2['index']=mcapTop2['date']+mcapTop2['Ticker']
    mcap_sec['index']=mcap_sec['date']+mcap_sec['ticker']
    mcapTop2=pd.merge(mcapTop2,mcap_sec[['index','primecode']],on='index',how='left')
    mcapTop2['primecode']=mcapTop2['primecode'].fillna(method='ffill')
    mcapTop_memb=mcapTop2.copy()
    newdaily=dailyreturn.loc[dailyreturn['ticker'].isin(mcapTop2['Ticker']),['date','ticker','dailyreturn']].copy()
    newdaily['index']=newdaily['date']+newdaily['ticker']
    mcapTop2=pd.merge(mcapTop2,newdaily[['index','dailyreturn']],on='index',how='left')
    mcapTop2['index']=mcapTop2['primecode']+mcapTop2['date']
    mcapTop2['sectop_return']=mcapTop2.groupby('index')['dailyreturn'].transform('mean')
    mcapTop2=mcapTop2.drop_duplicates(subset=['index'],keep='last')
    mcapTop2=mcapTop2[['date','primecode','sectop_return']].copy()
    mcapTop2=mcapTop2.sort_values(by=['date'],ascending=[True])
    return(mcapTop2,mcapTop_memb)

#rebaldaylist=DC.Rebaldaylist('2009-12-31',30)
#Buy the top 6 CITIC sectors with the highest coverage and take the mean return of it
def CITICsec_analyst(dailyreturn,rebaldaylist):
    CITICsectop_dailyreturn,mcapTop_memb=CITICsectop_return(dailyreturn,rebaldaylist)
    CITICsectop_dailyreturn['index']=CITICsectop_dailyreturn['date']+CITICsectop_dailyreturn['primecode']
    tradingday=pd.DataFrame(list(CITICsectop_dailyreturn['date'].unique()),columns=['date'])
    CSI,CITIC=SRDaily.Getsecname(dailyreturn,rebaldaylist,30)
    CITICtop=CITIC.groupby('date').apply(lambda x: x.nlargest(6,'coverage')).reset_index(drop=True)
    CITICtop=CITICtop.pivot_table(index='date',columns='sector',values='coverage',aggfunc='first')
    CITICtop=CITICtop.fillna(0)
    CITICtop=pd.merge(tradingday,CITICtop,on='date',how='left')
    CITICtop=CITICtop.fillna(method='ffill')
    CITICtop['date']=CITICtop['date'].shift(-1)
    CITICtop=CITICtop.loc[CITICtop['date'].isnull()==False,:].copy()
    CITICtop=pd.melt(CITICtop,id_vars='date',value_vars=list(CITICtop.columns[1:len(CITICtop.columns)+1]),var_name='sector',value_name='coverage')    
    CITICtop=CITICtop.loc[CITICtop['coverage']>0,:].copy()
    CITICtop=CITICtop.sort_values(by=['date'],ascending=[True])
    CITICtop['index']=CITICtop['date']+CITICtop['sector']
    topsectors=CITICtop.copy()
    CITICsectop_dailyreturn=CITICsectop_dailyreturn.loc[CITICsectop_dailyreturn['index'].isin(CITICtop['index']),:].copy()
    CITICsectop_dailyreturn['dailyreturn']=CITICsectop_dailyreturn.groupby(['date'])['sectop_return'].transform('mean')
    CITICsectop_dailyreturn=CITICsectop_dailyreturn.drop_duplicates(subset=['date'],keep='last')
    CITICsectop_dailyreturn=CITICsectop_dailyreturn[['date','dailyreturn']]
    mcapTop_memb['index']=mcapTop_memb['date']+mcapTop_memb['primecode']
    mcapTop_memb=mcapTop_memb.loc[(mcapTop_memb['index'].isin(topsectors['index'])),:].copy()
    mcapTop_memb=mcapTop_memb.sort_values(by=['date','primecode'],ascending=[True,True])
    mcapTop_memb=mcapTop_memb[['date','Ticker','mcap','primecode']]
    secname=DC.Sec_name('CITIC')
    secname=secname.rename(columns={'sector':'primecode'})
    mcapTop_memb=pd.merge(mcapTop_memb,secname,on='primecode',how='left')
    return(CITICsectop_dailyreturn,mcapTop_memb)

def Sector_return(tickerlist):
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    sql="select convert(varchar,TradingDay,23), SM.SecuCode,  ChangePCT/100 from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where  TradingDay>'2008-12-31' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
    reslist=ms.ExecQuery(sql)
    rechist=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
    rechist=rechist.sort_values(by=['date'],ascending=[True])
    rechist['dailyreturn']=rechist['dailyreturn'].astype('float')
    return(rechist)


def CSI_Rotation(dailyreturn,rebaldaylist):
    CSI,CITIC=SRDaily.Getsecname(dailyreturn,rebaldaylist,30)
    CSItop=CSI.groupby('date').apply(lambda x: x.nlargest(4,'coverage').reset_index(drop=True))
    CSItop.reset_index(drop=True,inplace=True)
    ticker=pd.read_csv("D:/SecR/sector_map.csv")
    ticker['ticker']=[str(x).zfill(6)for x in ticker['ticker']]
    tickerlist=list(ticker['ticker'].unique())
    sectorreturn=Sector_return(tickerlist)
    sectorreturn=pd.merge(sectorreturn,ticker[['ticker','sector']],on='ticker',how='left')
    tradingday=pd.DataFrame(list(sectorreturn['date'].unique()),columns=['date'])
    CSItop=CSItop.pivot_table(index='date',columns='sector',values='coverage',aggfunc='first')
    CSItop=CSItop.fillna(0)
    CSItop=pd.merge(tradingday,CSItop,on='date',how='left')
    CSItop=CSItop.fillna(method='ffill')
    CSItop['date']=CSItop['date'].shift(-1)
    CSItop=CSItop.loc[CSItop['date'].isnull()==False,:].copy()
    CSItop=pd.melt(CSItop,id_vars='date',value_vars=list(CSItop.columns[1:len(CSItop.columns)+1]),var_name='sector',value_name='coverage')
    CSItop=CSItop.loc[CSItop['coverage']>0,:].copy()
    CSItop=CSItop.sort_values(by=['date'],ascending=[True])
    CSItop['index']=CSItop['date']+CSItop['sector']
    sectorreturn['sector']=[str(x).zfill(2)for x in sectorreturn['sector']]
    sectorreturn['sector']=sectorreturn['sector'].astype('str')
    sectorreturn['index']=sectorreturn['date']+sectorreturn['sector']
    sectorreturn=sectorreturn.loc[sectorreturn['index'].isin(CSItop['index']),:].copy()
    sectorreturn['return']=sectorreturn.groupby('date')['dailyreturn'].transform('mean')
    sectorreturn=sectorreturn.drop_duplicates(subset=['date'],keep='last')
    sectorreturn=sectorreturn.sort_values(by=['date'],ascending=[True])
    sectorreturn['stratReturn']=np.exp(np.log1p(sectorreturn['return']).cumsum())
    sectorreturn=sectorreturn[['date','return','stratReturn']].copy()
    return(sectorreturn)

def moving_average(interval, windowsize):
    window = np.ones(int(windowsize)) / float(windowsize)
    re = np.convolve(interval, window, 'same')
    return re
 
def LabberRing():
    t = np.linspace(-4, 4, 100)   # np.linspace 等差数列,从-4到4生成100个数
    #print('t=', t)
 # np.random.randn 标准正态分布的随机数，np.random.rand 随机样本数值
    y = np.sin(t) + np.random.randn(len(t)) * 0.1   # 标准正态分布中返回1个，或者多个样本值
    #print('y=', y)
    CSI300=pd.read_csv("D:/MISC/CSI300.csv")
    CSI300=CSI300.loc[CSI300['Date']<='2020-01-01',:].copy()
    y=CSI300['CSI300']   
    y_av = moving_average(y, 120)
    plt.plot(y_av, 'b')
    CSI300['smooth']=y_av
    #plt.xlabel('Time')
    #plt.ylabel('Value')
    # plt.grid()网格线设置
    #plt.grid(True)
    plt.show()
    return(CSI300)

def RandomGenerate():
    base=0
    for i in range(1,13):
        ran=random.random()
        base=base+ran
    rand=base-6
    return(rand)

def Randomwalk(vol,drift):
    timestep=1/250
    pricelist=[100]
    returnlist=[]
    for i in range(0,2000):
        rand=RandomGenerate()
        dailyreturn=(1+drift*timestep+(vol)*math.sqrt(timestep)*rand)
        newprice=pricelist[i]*dailyreturn
        pricelist.append(newprice)
        returnlist.append(dailyreturn)
    pricelist=pd.DataFrame(pricelist,columns=['close'])
    std=np.std(returnlist)
    print(std*16)
    plt.plot(pricelist['close'])
    return(pricelist)

def Simulation():
    returnlist=[]
    for i in range(0,10000):
        a=Randomwalk(0.3,0.3)
        returnlist.append(a)
    mean=np.mean(returnlist)
    print(mean)
    return()


def SecIndexData():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    sectormap=pd.read_csv('D:/SecR/sector_map.csv')
    sectormap['sector']=sectormap['sector'].astype(str)
    sectormap['ticker']=sectormap['ticker'].astype(str)
    sectormap['sector']=[x.zfill(2) for x in sectormap['sector']]
    sectormap['ticker']=[x.zfill(6) for x in sectormap['ticker']]
    tickerlist=list(sectormap['ticker'].unique())
    sql="select convert(varchar,TradingDay,23), SM.SecuCode,  ChangePCT/100 from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where  TradingDay>'2006-12-31' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
    reslist=ms.ExecQuery(sql)
    indexreturn=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
    indexreturn=indexreturn.sort_values(by=['date'],ascending=[True])
    indexreturn['dailyreturn']=indexreturn['dailyreturn'].astype('float')
    return(indexreturn)

def Topstock():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    membhist=DC.Benchmark_membs('CSI300','2006-12-31')
    membhist['weightrank']=membhist.groupby(['date'])['weight'].rank("dense", ascending=False)
    membhist=membhist.loc[membhist['weightrank']<=50,:]
    tickerlist=list(membhist['ticker'].unique())
    sql="select convert(varchar,TradingDay, 23) as date, SM.SecuCode, ChangePCT from JYDBBAK.dbo.QT_Performance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay>'2007-01-01' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
    reslist=ms.ExecQuery(sql)
    stockreturn=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
    stockreturn['dailyreturn']=stockreturn['dailyreturn']/100
    membhist=membhist.pivot_table(index='date',columns='ticker',values='weight',aggfunc='first')
    membhist=membhist.reset_index(inplace=False)
    membhist=membhist.fillna(0)
    tradingday=pd.DataFrame(list(stockreturn['date'].unique()),columns=['date'])
    membhist=pd.merge(tradingday, membhist,on='date',how='left')
    membhist=membhist.fillna(method='ffill')
    membhist=pd.melt(membhist,id_vars=['date'],value_vars=list(membhist.columns[1:]),var_name='ticker',value_name='weight')
    membhist=membhist.loc[membhist['weight']>0,:].copy()
    membhist['index']=membhist['date']+membhist['ticker']
    stockreturn['index']=stockreturn['date']+stockreturn['ticker']
    stockreturn=stockreturn.loc[stockreturn['index'].isin(membhist['index']),:].copy()
    stockreturn=stockreturn.sort_values(by=['date','ticker'],ascending=[True,True])
    return(stockreturn)

def PerSector(seccode,dailyreturn):
    rebaldaylist=DC.Rebaldaylist('2007-01-04',30)
    stock=DC.Sector_stock(rebaldaylist,[seccode],'CSI')
    tempdailyreturn=dailyreturn.loc[dailyreturn['ticker'].isin(list(stock['ticker'].unique())),['date','ticker','mcap','dailyreturn']].copy()
    stock['index']=stock['date']+stock['ticker']
    tempdailyreturn['index']=tempdailyreturn['date']+tempdailyreturn['ticker']
    stock=stock.loc[stock['index'].isin(tempdailyreturn['index']),:]
    stock=pd.merge(stock,tempdailyreturn[['index','mcap','dailyreturn']],on='index',how='left')
    stock['rank']=stock.groupby(['date'])['mcap'].rank("dense", ascending=False)
    stock=stock.loc[stock['rank']<=40,:].copy()
    tradingday=pd.DataFrame(list(tempdailyreturn['date'].unique()),columns=['date'])
    stock=stock.pivot_table(index='date',columns='ticker',values='mcap',aggfunc='first')
    stock=stock.reset_index(inplace=False)
    stock=stock.fillna(0)
    stock=pd.merge(tradingday,stock,on='date',how='left')
    stock=stock.fillna(method='ffill')
    stock=pd.melt(stock,id_vars=['date'],value_vars=list(stock.columns[1:]),var_name='ticker',value_name='mcap')
    stock=stock.loc[stock['mcap']!=0,:].copy()
    stock=stock.sort_values(by=['date'],ascending=[True])
    stock['index']=stock['date']+stock['ticker']
    stock=pd.merge(stock,tempdailyreturn[['index','dailyreturn']],on='index',how='left')
    stock=stock.sort_values(by=['date'],ascending=[True])
    stock=stock[['date','ticker','dailyreturn']]
    return(stock)
    

def PCA_Analysis(indexreturn):
    indextab=indexreturn.pivot_table(index='date',columns='ticker',values='dailyreturn',aggfunc='first')
    indextab=indextab.reset_index(inplace=False)
    indextab=indextab.sort_values(by=['date'],ascending=[True])
    ERlist=[]
    datelist=[]
    for i in range(60,indextab.shape[0]-1):
        #print(i)
        tempreturntab=indextab.iloc[(i-60):i,1:].copy()
        tempreturntab=tempreturntab.dropna(axis=1,thresh=20)
        #tempreturntab=tempreturntab.fillna(tempreturntab.mean(axis=1),axis=1)
        tempreturntab=tempreturntab.T.fillna(tempreturntab.mean(axis=1)).T
        tempreturntab=tempreturntab.astype(float)
        #print(tempreturntab.shape)
        covmat=np.array(pd.DataFrame.cov(tempreturntab))
        sum_var=covmat.diagonal().sum()
        pca=PCA(n_components=5)
        pca.fit(tempreturntab)
        sum_eigen=sum(pca.explained_variance_)
        ExpRatio=sum_eigen/sum_var
        ERlist.append(ExpRatio)
        datelist.append(indextab.iloc[i,0])
    ERlist=pd.DataFrame({'date':datelist,'AR':ERlist})
    ERlist=ERlist.loc[ERlist['date']>='2007-01-01',:].copy()
    return(ERlist)

#计算持仓时期的max return....这个max测量范围为这一期持仓到目前已经过去了多少天
def Lookback(row,values,lookbackref,method='max',*args,**kwargs):
    loc=values.index.get_loc(row.name)
    pl=lookbackref.loc[row.name]
    period_max=getattr(values.iloc[loc-(pl-1):(loc+1)],method)(*args, **kwargs)
    return(period_max)

#test['new_col']=test.apply(Lookback,values=test['culreturn'],lookbackref=test['window'],axis=1)

def Period_maxdd():
    testfile=pd.read_csv("D:/MISC/test.csv")           #导入历史回报
    testfile['poschange']=testfile['position']-testfile['position'].shift(1)   #把买入点标记出来，1为买入，-1为卖出
    testfile.loc[testfile['poschange']==-1,'poschange']=0                      #只保留买入点
    if testfile['position'][0]==1:
        testfile.loc[0,'poschange']=1
    else:
        testfile.loc[0,'poschange']=0                                          #如果表格第一天没持仓，则定性为0，有持仓为1
    testfile['culreturn']=np.exp(np.log1p(testfile['dailyreturn']).cumsum())   #计算历史cumulative return
    testfile['trade_cul']=testfile['poschange']*testfile['culreturn']          #用0/1去乘cumulative return，只有买入那天的cumulative return会被保留；其余为0
    testfile['trade_cul']=testfile['trade_cul'].replace(to_replace=0,method='ffill') #把0换成last return. 则这一列全都是上一次买入那天当天的cumulative return
    testfile['period_drawdown']=testfile['culreturn']/testfile['trade_cul']-1        #用截止今天cumulativereturn/上一次买入当天的cumulative return就是持仓这段时间的cumulative return啦；再减1就是drawdown
    testfile['position2']=testfile['position'].shift(1)                              #因为是收盘下单，所以把标记往后挪一天
    testfile.loc[testfile['position2']==0,'period_drawdown']=0                       #不持仓的时候最大跌幅变为0
    testfile=testfile[['date','dailyreturn','position','period_drawdown']]           
    return(testfile)

def Period_highdd():
    testfile=pd.read_csv("D:/MISC/test.csv")                                    #导入历史回报
    testfile['cumpos']=testfile['position'].cumsum()                            #把所有position的0/1都叠加起来
    testfile['lastcum']=np.nan                                                  
    testfile['temp']=testfile['cumpos'].shift(1)                            
    testfile.loc[testfile['position']==0,'lastcum']=testfile.loc[testfile['position']==0,'temp'] #lastcum=上期持仓结束的时候累计总天数
    testfile.loc[0,'lastcum']=0                                                   
    testfile['lastcum']=testfile['lastcum'].fillna(method='ffill')              #如果今天不持仓，上期关仓时的累计天数为t-1累计天数；如果今天持仓。上期关仓累计天数跟着last row即可
    testfile['period_cum']=testfile['cumpos']-testfile['lastcum']               #本期开始到现在已经持仓了多少天=本期总累计天数-上期持仓结束前的累计总天数    
    testfile['period_cum']=testfile['period_cum'].astype(int)
    testfile['period_maxcul']=testfile.apply(Lookback,values=testfile['culreturn'],lookbackref=testfile['period_cum'],axis=1)
    testfile['period_highdd']=testfile['culreturn']-testfile['period_maxcul'].shift(1)
    return(testfile)


    
        