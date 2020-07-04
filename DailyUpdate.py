# -*- coding: utf-8 -*-
"""
Created on Wed May 27 13:29:40 2020

@author: wudi
"""

import pandas as pd
import numpy as np
from FundaStock import Funda as FundaStockFund
from FundaStock import Prep as FundaStockPrep
from Toolbox import DataCollect 
from Toolbox import WeightScheme
from HotStock import Prep as HSPrep
from HotStock import Review as HSReview
from HotStock import SecR as SR
from AnalystStock import Top_analyst as ASTA
from AnalystStock import Niu2 as ASNiu2
from AnalystStock import Prep as APrep
from BankFunda import Prep as BankFP
from Toolbox import DataStructuring 
from scipy import stats
from Quant import Otho 

DC=DataCollect()
FP=FundaStockPrep()
FF=FundaStockFund()
HP=HSPrep()
HR=HSReview()
TA=ASTA()
N=ASNiu2()
AP=APrep()
BFP=BankFP()
WS=WeightScheme()
DS=DataStructuring()
OT=Otho()
SRDaily=SR()

N.Analyst_history()
print("analyst_history_updated")
DC.Tradingday()
DC.Dailyreturn_Update_Daily()
dailyreturn=DC.Dailyreturn_retrieve()
today=dailyreturn['date'].max()
rebalday=[str(today)[0:10]]

#facdict={'Quality': ['ROETTM', 'ROATTM','GrossIncomeRatioTTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS']}
#rebalday=['2020-06-01']
def DailyUpdate(dailyreturn,rebalday):
    facdict={'Quality': ['ROETTM', 'ROATTM','GrossIncomeRatioTTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS']}
    selectsigs=[]
    [selectsigs.extend(v) for k, v in facdict.items()]
    siglist=list(set([x.replace('growth','') for x in selectsigs]))
    siglist=list(set([x.replace('vol','') for x in siglist]))
    facnamelist=list(facdict.keys())
    sighist=FP.SigdataPrep(dailyreturn,siglist,rebalday)
    sighist=DS.GrowVol(sighist,'grow')
    nsigdict=FF.NSighist(dailyreturn,rebalday,sighist,selectsigs)
    fzdict={}
    for facname in facnamelist:
        siginfac=facdict[facname]
        fzdict=FF.Factorscore(rebalday,nsigdict,facname,siginfac,fzdict) 
    gentable=pd.DataFrame()
    for facname in facnamelist:
        df=fzdict[facname+'_'+rebalday[0]][['ticker',facdict[facname][0],'N_'+facdict[facname][0],'Q',facdict[facname][0]+'_zscore']]
        #df=fzdict[facname+'_'+rebalday[0]]
        df=df.rename(columns={'Q':facdict[facname][0]+'_Q'})
        #df=df.rename(columns={'Q':facname+'_Q'})
        if gentable.shape[0]==0:
            gentable=df.copy()
        else:
            gentable=pd.merge(gentable,df,on='ticker',how='outer')
    return(gentable)

#Update with more metrics in the Factor
def DailyUpdate2(dailyreturn,rebalday):
    #facdict={'Quality': ['ROETTM'],'Growth': ['QRevenuegrowth'],'Value': ['PE'],'Market':['turnoverweek']}
    facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
    selectsigs=[]
    [selectsigs.extend(v) for k, v in facdict.items()]
    siglist=list(set([x.replace('growth','') for x in selectsigs]))
    siglist=list(set([x.replace('vol','') for x in siglist]))
    facnamelist=list(facdict.keys())
    sighist=FP.SigdataPrep(dailyreturn,siglist,rebalday)
    sighist=DS.GrowVol(sighist,'grow')
    nsigdict=FF.NSighist(dailyreturn,rebalday,sighist,selectsigs)
    fzdict={}
    for facname in facnamelist:
        siginfac=facdict[facname]
        fzdict=FF.Factorscore(rebalday,nsigdict,facname,siginfac,fzdict) 
    gentable=pd.DataFrame()
    for facname in facnamelist:
        #df=fzdict[facname+'_'+rebalday[0]][['ticker',facdict[facname][0],'N_'+facdict[facname][0],'Q',facdict[facname][0]+'_zscore']]
        df=fzdict[facname+'_'+rebalday[0]]
        #df=df.rename(columns={'Q':facdict[facname][0]+'_Q'})
        df=df.rename(columns={'Q':facname+'_Q'})
        if gentable.shape[0]==0:
            gentable=df.copy()
        else:
            gentable=pd.merge(gentable,df,on='ticker',how='outer')
    return(gentable)

def Bank_Fundatable():
    sighist=BFP.Bank_download()
    sighist=sighist.sort_values(by=['ticker','enddate'],ascending=[True,True])
    sighist['index']=sighist['ticker']+sighist['signame']
    sighist=sighist.drop_duplicates(['index'],keep="last")
    bankpivot=sighist.pivot_table(index='ticker',columns='signame',values='sigvalue',aggfunc='first')
    bankpivot.reset_index(inplace=True)
    bankpivot.columns=['ticker','拨贷比','拨备覆盖率','不良贷款率','净利差','净息差','杠杆率','核心一级资本充足率']
    return(bankpivot)

def Generate_Hostock(rebaldaylist):
    rechist=HP.Hotstock_nonsectorQuery(rebaldaylist,60)
    rechist=rechist.rename(columns={'raccount':'RecomCounts'})
    return(rechist)
    

def Combine(dailyreturn,rebalday):
    print('executing update')
    gentable=DailyUpdate2(dailyreturn,rebalday)
    rechist=Generate_Hostock(rebalday)
    gentable=pd.merge(gentable,rechist[['ticker','RecomCounts']],on='ticker',how='left')
    gentable=pd.merge(gentable,dailyreturn.loc[dailyreturn['date']==rebalday[0],['ticker','mcap']],on='ticker',how='left')
    gentable['ticker']=[x+' CH' for x in gentable['ticker']]
    gentable.to_csv("D:/CompanyData/Gentable_"+rebalday[0]+".csv",index=False)
    companydata=gentable[['ticker','Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].copy()
    companydata.to_csv("D:/CompanyData/CompanyData_"+rebalday[0]+".csv",index=False)
    otho=Othogonization(gentable)
    otho.to_csv("D:/CompanyData/Otho_"+rebalday[0]+".csv",index=False)
    return(gentable)
    
    
def Getcurrentholding(topanalsyt,dailyreturn,rebaldaylist):
    df_holding=AP.Holding_query(topanalsyt)
    df_holding=AP.clean_up_ticker(df_holding)
    df_holding=df_holding[['date','ticker']]
    df_holding=df_holding.drop_duplicates()
    df_holding_active=WS.Active_stock_screening(df_holding,dailyreturn,rebaldaylist)
    return(df_holding_active)

def Generate_Analystpicks(dailyreturn):
    daylist=list(dailyreturn['date'].unique())
    daylist.sort()
    analystrebalday=daylist[-7]                                                            #7 trading days ago
    rebaldaylist=[str(analystrebalday)[0:10]]
    activepickNS,TAH,top30p=TA.Top_analyst_nonSector(dailyreturn,rebaldaylist,60,'CSI800') #activeNS,skipped the intersection part
    holding_top30p=Getcurrentholding(top30p,dailyreturn,rebaldaylist)
    holding_top30p['top30percent']=1
    activepicksSec,TAH2,top5=TA.Top_analyst_Sector(dailyreturn,rebaldaylist,60,'CSI800')   #activeNS,skipped the intersection part
    holding_top5=Getcurrentholding(top5,dailyreturn,rebaldaylist)
    holding_top5['sectorReturnTop5']=1
    activepickNiu2,TAH3,Niu2top5=N.Top_analyst_Niu2(dailyreturn,rebaldaylist,'CSI800')
    holding_Niu2=Getcurrentholding(Niu2top5,dailyreturn,rebaldaylist)
    holding_Niu2['sectorAlphaTop5']=1
    holding_Hostock=HR.ActivepickBMSec_production(dailyreturn,'CSIAll',rebaldaylist)
    holding_Hostock['HostockCSI800']=1
    tickerlist=list(set(holding_top30p['ticker']).union(set(holding_top5['ticker'])))
    tickerlist=list(set(tickerlist).union(set(holding_Niu2['ticker'])))
    analyst_pick=pd.DataFrame(tickerlist,columns=['ticker'])
    analyst_pick=pd.merge(analyst_pick,holding_top30p[['ticker','top30percent']],on='ticker',how='left')
    analyst_pick=pd.merge(analyst_pick,holding_top5[['ticker','sectorReturnTop5']],on='ticker',how='left')
    analyst_pick=pd.merge(analyst_pick,holding_Niu2[['ticker','sectorAlphaTop5']],on='ticker',how='left')
    analyst_pick=pd.merge(analyst_pick,holding_Hostock[['ticker','HostockCSI800']],on='ticker',how='left')
    analyst_pick.to_csv("D:/CompanyData/Analyst_pick_"+rebaldaylist[0]+".csv",index=False)
    return(analyst_pick)

def Othogonization(gentable):
    newdf=gentable[['ticker','Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].copy()
    newdf=newdf.dropna()
    newdf=newdf.reset_index(drop=True)
    tobeo=np.array(newdf.iloc[:,1:])
    otho=pd.DataFrame(OT.Gram_Schmidt(tobeo),columns=['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore'])
    otho.insert(0,'ticker',newdf['ticker'])
    otho[['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']]=otho[['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].apply(lambda x:stats.zscore(x))
    return(otho)
    
def SecRDaily():
    rebaldaylist=rebalday
    topsecname=SRDaily.Getsecname(dailyreturn,rebaldaylist,60)
    topsecname=topsecname.sort_values(by=['raccount'])
    topsecname=topsecname[['date','sector','sectorname','raccount']]
    topsecname.to_csv("D:/CompanyData/TopSector_"+rebaldaylist[0]+".csv",encoding='utf-8-sig',index=False)
    return(topsecname)

gentable=Combine(dailyreturn,rebalday)
AP=Generate_Analystpicks(dailyreturn)
topsecname=SecRDaily()