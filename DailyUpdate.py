# -*- coding: utf-8 -*-
"""
Created on Wed May 27 13:29:40 2020

@author: wudi
"""

import pandas as pd
import numpy as np
import datetime
from FundaStock import Funda as FundaStockFund
from FundaStock import Prep as FundaStockPrep
from FundaStock import FactorReturn as FundaFacReturn
from Toolbox import DataCollect 
from Toolbox import WeightScheme
from HotStock import Prep as HSPrep
from HotStock import Review as HSReview
from HotStock import SecR as SR
from AnalystStock import Top_analyst as ASTA
from AnalystStock import Niu2 as ASNiu2
from AnalystStock import Prep as APrep
from AnalystStock import Review as AReview
from BankFunda import Prep as BankFP
from Toolbox import DataStructuring 
from scipy import stats
from Quant import Otho
from datetime import date
from dateutil.relativedelta import relativedelta, TH 

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
AR=AReview()
FFR=FundaFacReturn()

N.Analyst_history()
print("analyst_history_updated")
DC.Tradingday()
DC.Dailyreturn_Update_Daily()
dailyreturn=DC.Dailyreturn_retrieve()
lasttradingday=dailyreturn['date'].max()
rebalday=[str(lasttradingday)[0:10]]

today=date.today()
if datetime.datetime.today().weekday()>=3:
    thursday=today+relativedelta(weekday=TH(-2))
else:
    thursday=today+relativedelta(weekday=TH(-1))
thursday=[str(thursday.strftime("%Y-%m-%d"))]
todayname=str(today.strftime("%Y-%m-%d"))

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
    nsigdict=FF.NSighist(dailyreturn,rebalday,sighist,selectsigs,'','N')
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
    nsigdict=FF.NSighist(dailyreturn,rebalday,sighist,selectsigs,'','N')
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
    
#Produce the Shen5 Company Data
def Combine(dailyreturn,rebalday):
    print('executing update')
    gentable=DailyUpdate2(dailyreturn,rebalday)
    rechist=Generate_Hostock(rebalday)
    gentable=pd.merge(gentable,rechist[['ticker','RecomCounts']],on='ticker',how='left')
    gentable=pd.merge(gentable,dailyreturn.loc[dailyreturn['date']==rebalday[0],['ticker','mcap']],on='ticker',how='left')
    gentable['ticker']=[x+' CH' for x in gentable['ticker']]
    gentable.to_csv("D:/CompanyData/Gentable_"+todayname+".csv",index=False)
    companydata=gentable[['ticker','Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].copy()
    companydata.to_csv("D:/CompanyData/CompanyData_"+todayname+".csv",index=False)
    otho=Othogonization(gentable)
    otho.to_csv("D:/CompanyData/Otho_"+todayname+".csv",index=False)
    return(gentable)
    
    
def Getcurrentholding(topanalsyt,dailyreturn,rebaldaylist):
    df_holding=AP.Holding_query(topanalsyt)
    df_holding=AP.clean_up_ticker(df_holding)
    df_holding=df_holding[['date','ticker']]
    df_holding=df_holding.drop_duplicates()
    df_holding_active=WS.Active_stock_screening(df_holding,dailyreturn,rebaldaylist)
    return(df_holding_active)


def Othogonization(gentable):
    newdf=gentable[['ticker','Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].copy()
    newdf=newdf.dropna()
    newdf=newdf.reset_index(drop=True)
    tobeo=np.array(newdf.iloc[:,1:])
    otho=pd.DataFrame(OT.Gram_Schmidt(tobeo),columns=['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore'])
    otho.insert(0,'ticker',newdf['ticker'])
    otho[['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']]=otho[['Quality_zscore','Value_zscore','Growth_zscore','Market_zscore']].apply(lambda x:stats.zscore(x))
    return(otho)

#Produce the Hot Sectors (CSI and CITIC)
def SecRDaily():
    rebaldaylist=rebalday
    CSI,CITIC=SRDaily.Getsecname(dailyreturn,rebaldaylist,60)
    CSI,CITIC=map(lambda df: df.sort_values(by=['raccount'],ascending=[False]),[CSI,CITIC])
    CSI,CITIC=map(lambda df: df[['date','sector','sectorname','raccount']],[CSI,CITIC])
    CSI.to_csv("D:/CompanyData/CSITopSector_"+todayname+".csv",encoding='utf-8-sig',index=False)
    CITIC.to_csv("D:/CompanyData/CITICTopSector_"+todayname+".csv",encoding='utf-8-sig',index=False)
    return(CSI,CITIC)

#Produce the Shen5 and SHen5 (3+4)stocks list
def Shen5():
     benchmark='CSIAll'
     lookback_period=60
     rebaldaylist=thursday
     N2,Sec,NS,HS=AR.TApostab(dailyreturn,rebaldaylist,lookback_period,benchmark)
     N2['sectorAlphaTop5'],Sec['sectorReturnTop5'],NS['top30percent'],HS['HostockCSIAll']=[1,1,1,1]
     resultlist=[N2['ticker'],Sec['ticker'],NS['ticker'],HS['ticker']]
     tickerlist=set().union(*resultlist)
     analyst_pick=pd.DataFrame(tickerlist,columns=['ticker'])
     analyst_pick=pd.merge(analyst_pick,NS[['ticker','top30percent']],on='ticker',how='left')
     analyst_pick=pd.merge(analyst_pick,Sec[['ticker','sectorReturnTop5']],on='ticker',how='left')
     analyst_pick=pd.merge(analyst_pick,N2[['ticker','sectorAlphaTop5']],on='ticker',how='left')
     analyst_pick=pd.merge(analyst_pick,HS[['ticker','HostockCSIAll']],on='ticker',how='left')
     N2,Sec,NS,HS=map(DS.Addindex,(N2,Sec,NS,HS))
     N2,Sec,NS,HS=map(lambda df: df[['date','ticker','index']],[N2,Sec,NS,HS])
     frames=[N2,Sec,NS,HS]
     postab=pd.concat(frames)
     TAcount=pd.DataFrame(postab.groupby('index')['index'].count())
     TAcount=TAcount.rename(columns={'index':'count'})
     TAcount.reset_index(inplace=True)
     TAcount=TAcount.loc[TAcount['count']>=3,:]
     TAcount['date']=TAcount['index'].str[0:10]
     TAcount['ticker']=TAcount['index'].str[10:]
     TAcount=TAcount[['date','ticker','count']]
     analyst_pick.to_csv("D:/CompanyData/Analyst_pick_"+todayname+".csv",index=False)
     TAcount.to_csv("D:/CompanyData/Shen5Three_Four_"+todayname+".csv",index=False)
     return(TAcount,analyst_pick)

#Produce the Q5 stocks of Shen567
def Shen567(TAcount,rebalday):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        #ThreeFour=pd.read_csv("D:/SecR/ThreeFour.csv")
        #ThreeFour=pd.read_csv("D:/SecR/Shen56.csv")
        Shen6stocks=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
        Shen6stocks=Shen6stocks.loc[Shen6stocks['date']==max(Shen6stocks['date']),:]
        Shen6stocks=Shen6stocks.loc[~Shen6stocks['ticker'].str.contains('.HK'),:]
        Shen6=Shen6stocks['ticker'].str[0:6]
        ThreeFour=TAcount.copy()
        ThreeFour['ticker']=[str(x).zfill(6)for x in ThreeFour['ticker']]
        ThreeFour=ThreeFour.drop_duplicates()
        newThreeFour=list(ThreeFour['ticker'])
        newThreeFour.extend(list(Shen6))
        newThreeFour=pd.DataFrame(list(set(newThreeFour)),columns=['ticker'])
        newThreeFour['date']=rebalday[0]
        newThreeFour=newThreeFour.drop_duplicates()
        rebaldaylist=rebalday
        fzdict=FF.Fzdict(dailyreturn,rebaldaylist,facdict,newThreeFour,'ThreeFour')                                                             
        fztab=FF.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        olddict={}
        for rebalday in rebaldaylist:
            rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
            rebalz['rank']=rebalz['meanscore'].rank(method='first')
            rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
            olddict['meanscore_'+rebalday]=rebalz
        Shen567Q5=olddict['meanscore_'+rebalday].loc[olddict['meanscore_'+rebalday]['Q']==5,['date','ticker','meanscore']]
        Shen567Q5['ticker']=[str(x).zfill(6)+" CH" for x in Shen567Q5['ticker']]
        portdict=FF.Portdict(olddict,rebaldaylist)                                               ##calculate added zscore of factor and Group it into 5Q,Generate each Factor's holdings
        Shen567Q5.to_csv("D:/CompanyData/Shen567Q5_"+todayname+".csv",index=False)
        return(portdict)

def FactorReturnDaily():
    factorreturnhist=FFR.Facreturn_daily(dailyreturn)
    factorreturnhist.to_csv("D:/CompanyData/factoreturnhist.csv",index=False)
    return

gentable=Combine(dailyreturn,rebalday)
TAcount,analyst_pick=Shen5()
CSI,CITIC=SecRDaily()
facreturnhist=FFR.Facreturn_daily(dailyreturn)
portdict=Shen567(TAcount,rebalday)
FactorReturnDaily()