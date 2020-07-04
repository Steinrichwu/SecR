# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 10:50:01 2020

@author: wudi
"""
import pandas as pd
from Toolbox import DataCollect
from Toolbox import DataStructuring
DC=DataCollect()
DS=DataStructuring()

def Shen567(ifunion):
    rebal_period=20
    shen6=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
    startdate=shen6['date'].min()
    rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
    olddaylist=list(shen6['date'].unique())
    daylistmerge=DS.Daymerge(olddaylist,rebaldaylist)
    daydict=dict(zip(daylistmerge['date'],daylistmerge['oldday']))
    shen6=shen6.loc[~shen6['ticker'].str.contains('.HK'),:]
    shen6['ticker']=shen6['ticker'].str[0:6]
    ThreeFour=pd.read_csv("D:/SecR/Analyst34stocks_2007_WD.csv")
    ThreeFour=ThreeFour.loc[ThreeFour['date']>=startdate,:]
    ThreeFour['ticker']=[str(x).zfill(6) for x in ThreeFour['ticker']]
    olddaylist=list(ThreeFour['date'].unique())
    daylistmerge2=DS.Daymerge(olddaylist,rebaldaylist)
    daydict2=dict(zip(daylistmerge2['date'],daylistmerge2['oldday']))
    newuniversetab=pd.DataFrame(columns=['ticker','date'])
    for rebalday in rebaldaylist[1:]:
        Threefourdate=daydict2[rebalday]
        Threefourmemb=list(ThreeFour.loc[ThreeFour['date']==Threefourdate,'ticker'])
        shen6date=daydict[rebalday]
        shen6memb=list(shen6.loc[shen6['date']==shen6date,'ticker'])
        if ifunion=='Y':
            newuniverse=list(set(Threefourmemb).union(shen6memb))
        else:
            newuniverse=list(set(Threefourmemb)&set(shen6memb))
        newuniverse=pd.DataFrame(newuniverse,columns=['ticker'])
        newuniverse['date']=rebalday
        newuniversetab=newuniversetab.append(newuniverse)
    newuniversetab=newuniversetab[['date','ticker']]
    return(newuniversetab)

def TFIntersection(portdict,ThreeFour):
    Q5=portdict['meanscore_5'].copy()
    TF=ThreeFour.copy()
    Q5=Q5[['date','ticker']]
    TF=TF[['date','ticker']]
    Q5['newindex']=Q5['date']+Q5['ticker']
    TF['newindex']=TF['date']+TF['ticker']
    newQ5=Q5.append(TF)
    newQ5=newQ5.drop_duplicates()
    newQ5=newQ5[['date','ticker']]
    cpnl=RC.EqReturn(dailyreturn,newQ5)
    return(cpnl)
    
def fztabNEW(fztab,ThreeFour):
    facinfacz=['Market_zscore','Quality_zscore','Growth_zscore','Value_zscore','Analyst_zscore']
    TF=ThreeFour.copy()
    fztabtest=fztab.copy()
    fztabtest['newindex']=fztabtest['date']+fztabtest['ticker']
    TF['newindex']=TF['date']+TF['ticker']
    TF['Analyst_zscore']=0.5
    fztabtest=pd.merge(fztabtest,TF[['newindex','Analyst_zscore']],on='newindex',how='left')
    fztabtest['Analyst_zscore']=fztabtest['Analyst_zscore'].fillna(0)
    fztabtest['meanscore']=np.nanmean(fztabtest[facinfacz],axis=1)
    olddict={}
    for rebalday in rebaldaylist:
        rebalz=fztabtest.loc[fztabtest['date']==rebalday,:].copy()
        rebalz['rank']=rebalz['meanscore'].rank(method='first')
        rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
        olddict['meanscore_'+rebalday]=rebalz
    portdict=self.F.Portdict(olddict,rebaldaylist)
    PNLcumdict=self.P.PNLC(dailyreturn,portdict)             
    return(PNLcumdict)

    
