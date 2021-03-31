
# -*- coding: utf-8 -*-
"""
Created on Wed May 27 13:29:40 2020

@author: wudi
"""

import pandas as pd
import numpy as np
import datetime
import math
from FundaStock import Funda as FundaStockFund
from FundaStock import Prep as FundaStockPrep
from FundaStock import FactorReturn as FundaFacReturn
from Toolbox import DataCollect 
from Toolbox import WeightScheme
from HotStock import Prep as HSPrep
from HotStock import Review as HSReview
from HotStock import SecR as SR
from HotStock import Sectop_stocks as SecS
from AnalystStock import Top_analyst as ASTA
from AnalystStock import Niu2 as ASNiu2
from AnalystStock import Prep as APrep
from AnalystStock import Review as AReview
from Toolbox import DataStructuring 
from scipy import stats
from Quant import Otho
from datetime import date
from dateutil.relativedelta import relativedelta, TH 
from Portfolio import DailyProduction as DP
from Port import PortConst 
from STIB import Analysis as SA
from IPO import IPO as IPO

DC=DataCollect()
FP=FundaStockPrep()
FF=FundaStockFund()
HP=HSPrep()
HR=HSReview()
TA=ASTA()
N=ASNiu2()
AP=APrep()
WS=WeightScheme()
DS=DataStructuring()
OT=Otho()
SRDaily=SR()
AR=AReview()
FFR=FundaFacReturn()
PDP=DP()
PC=PortConst()
SAA=SA()
SS=SecS()
I=IPO()

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
thursday=['2021-01-21']
#facdict={'Quality': ['ROETTM', 'ROATTM','GrossIncomeRatioTTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS']}

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
    sighistRev=sighist.loc[sighist['signame']=='QRevenue',:].copy()
    sighistRev=sighistRev.sort_values(by=['ticker','publdate'],ascending=[True,True])
    sighistRev=sighistRev.drop_duplicates('ticker','last')
    sighistRev['enddate']=[x.strftime("%Y-%m-%d") for x in sighistRev['enddate']]
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
    gentable=pd.merge(gentable,sighistRev[['ticker','enddate','publdate']],on='ticker',how='left')
    return(gentable)


def DailyUpdateFullUniverse(dailyreturn,rebalday):
    facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
    selectsigs=[]
    [selectsigs.extend(v) for k, v in facdict.items()]
    siglist=list(set([x.replace('growth','') for x in selectsigs]))
    siglist=list(set([x.replace('vol','') for x in siglist]))
    facnamelist=list(facdict.keys())
    sighist=FP.SigdataPrep(dailyreturn,siglist,rebalday)
    sighist=DS.GrowVol(sighist,'grow')
    nsigdict=FF.NSighistFullUniverse(dailyreturn,rebalday,sighist,selectsigs,'','N')
    fzdict={}
    sighistRev=sighist.loc[sighist['signame']=='QRevenue',:].copy()
    sighistRev=sighistRev.sort_values(by=['ticker','publdate'],ascending=[True,True])
    sighistRev=sighistRev.drop_duplicates('ticker','last')
    sighistRev['enddate']=[x.strftime("%Y-%m-%d") for x in sighistRev['enddate']]
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
    gentable=pd.merge(gentable,sighistRev[['ticker','enddate','publdate']],on='ticker',how='left')
    return(gentable)



#def Bank_Fundatable():
#    sighist=BFP.Bank_download()
#    sighist=sighist.sort_values(by=['ticker','enddate'],ascending=[True,True])
#    sighist['index']=sighist['ticker']+sighist['signame']
#    sighist=sighist.drop_duplicates(['index'],keep="last")
#    bankpivot=sighist.pivot_table(index='ticker',columns='signame',values='sigvalue',aggfunc='first')
#    bankpivot.reset_index(inplace=True)
#    bankpivot.columns=['ticker','拨贷比','拨备覆盖率','不良贷款率','净利差','净息差','杠杆率','核心一级资本充足率']
#    return(bankpivot)

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

def Combine_FullUniverse(dailyreturn,rebalday):
    print('executing update')
    gentable=DailyUpdateFullUniverse(dailyreturn,rebalday)
    rechist=Generate_Hostock(rebalday)
    gentable=pd.merge(gentable,rechist[['ticker','RecomCounts']],on='ticker',how='left')
    gentable=pd.merge(gentable,dailyreturn.loc[dailyreturn['date']==rebalday[0],['ticker','mcap']],on='ticker',how='left')
    gentable['ticker']=[x+' CH' for x in gentable['ticker']]
    gentable.to_csv("D:/CompanyData/GentableFullUni_"+todayname+".csv",index=False)
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
    CSI,CITIC=map(lambda df: df.sort_values(by=['coverage'],ascending=[False]),[CSI,CITIC])
    #CSI,CITIC=map(lambda df: df[['date','sector','sectorname','raccount']],[CSI,CITIC])
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
     TAcount.to_csv("D:/SecR/ThreeFourlatest.csv",index=False)
     return(TAcount,analyst_pick)
     
def Shen56_Union(TAcount):
    Shen6stocks=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
    Shen6stocks=Shen6stocks.loc[Shen6stocks['date']==max(Shen6stocks['date']),:]
    Shen6stocks=Shen6stocks.loc[~Shen6stocks['ticker'].str.contains('.HK'),:]
    Shen6=Shen6stocks['ticker'].str[0:6]
    ThreeFour=TAcount.copy()
    ThreeFour['ticker']=[str(x).zfill(6)for x in ThreeFour['ticker']]
    ThreeFour=ThreeFour.drop_duplicates()
    newThreeFour=list(ThreeFour['ticker'])
    newThreeFour.extend(list(Shen6))
    return(newThreeFour)

#Fzdict(dailyreturn,rebaldaylist,facdict,universe,universetype)                      
#Produce the Q5 stocks of Shen567
def Shen567(TAcount,rebalday):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        #ThreeFour=pd.read_csv("D:/SecR/ThreeFour.csv")
        #ThreeFour=pd.read_csv("D:/SecR/Shen56.csv")
        newThreeFour=Shen56_Union(TAcount)
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
        Shen567Q5['meanscore']=[.5*(math.erf(x/2**.5)+1) for x in Shen567Q5['meanscore']]
        Shen567Q5['ticker']=[str(x).zfill(6)+" CH" for x in Shen567Q5['ticker']]
        portdict=FF.Portdict(olddict,rebaldaylist)                                               ##calculate added zscore of factor and Group it into 5Q,Generate each Factor's holdings
        Shen567Q5.to_csv("D:/CompanyData/Shen567Q5_"+todayname+".csv",index=False)
        return(portdict)
        
        
def SuperTrendDaily(CSIsecnum,TAcount,rebalday):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        rebaldaylist=rebalday
        universe=DC.Ashs_stock_seccode(rebaldaylist,[CSIsecnum],'CSI')
        fzdict=FF.Fzdict(dailyreturn,rebaldaylist,facdict,universe,'Sector')                                                             
        fztab=FF.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        olddict={}
        newuniversetab=pd.DataFrame(columns=['ticker','date'])
        for rebalday in rebaldaylist:
            rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
            rebalz['rank']=rebalz['meanscore'].rank(method='first')
            rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
            olddict['meanscore_'+rebalday]=rebalz
            shen7newmemb=olddict['meanscore_'+rebalday].loc[olddict['meanscore_'+rebalday]['Q']==5,['date','ticker','meanscore']]
            newThreeFour=Shen56_Union(TAcount)
            ensembleuniverse=list(set(shen7newmemb).union(newThreeFour))
            Secrebalstocks=list(universe['ticker'].unique())
            newuniverse=list(set(ensembleuniverse)&set(Secrebalstocks))
            newuniverse=pd.DataFrame(newuniverse,columns=['ticker'])
            newuniverse['date']=rebalday
            newuniversetab=newuniversetab.append(newuniverse)
        newdf=WS.Generate_PortNavMcap(newuniversetab,dailyreturn)
        return(newdf)

def SuperTrendSectorsDaily(TAcount,rebalday):
        newdf=SuperTrendDaily('04',TAcount,rebalday)
        newdf.to_csv("D:/CompanyData/ConsumerStaple_"+todayname+".csv",index=False)
        newdf=SuperTrendDaily('05',TAcount,rebalday)
        newdf.to_csv("D:/CompanyData/Healthcare_"+todayname+".csv",index=False)
        newdf=SuperTrendDaily('07',TAcount,rebalday)
        newdf.to_csv("D:/CompanyData/Tech_"+todayname+".csv",index=False)
        return()
        
def FactorReturnDaily():
    factorreturnhist=FFR.Facreturn_daily(dailyreturn)
    factorreturnhist.to_csv("D:/CompanyData/factoreturnhist.csv",index=False)
    return

def SectorValueDaily(dailyreturn):
    dfdaily=DC.Daily_sector_valuation_median_update(dailyreturn)
    dfdaily.to_csv("D:/CompanyData/Sector_valuation_"+todayname+".csv",encoding='utf-8-sig',index=False)
    return

def Shen56stockPool(TAcount):
    Shen5=TAcount[['date','ticker']].copy()
    Shen6=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
    Shen6latest=Shen6.loc[Shen6['date']==(Shen6['date'].max()),:].copy()
    Shen6latest['ticker']=[str(x)[0:6]for x in Shen6latest['ticker']]
    Shen56=Shen5.append(Shen6latest)
    Shen56['date']=Shen56['date'].max()
    Shen56=Shen56.drop_duplicates()
    OldShen56=pd.read_csv("D:/SecR/Shen56New.csv")
    OldShen56=OldShen56.append(Shen56)
    OldShen56=OldShen56.drop_duplicates()
    OldShen56.to_csv("D:/SecR/Shen56New.csv",index=False)
    return(Shen56)

def CompositePort(daiyreturn,rebalday):
    port,Ab,Ba,pickhist=PDP.CompositePort_Daily(dailyreturn,rebalday)
    Ab['ticker']=[str(x)+' CH' for x in Ab['ticker']]
    Ba['ticker']=[str(x)+' CH' for x in Ba['ticker']]
    pickhist['ticker']=[str(x)+' CH' for x in pickhist['ticker']]   
    port['ticker']=[str(x)+' CH' for x in port['ticker']]   
    Ab,Ba,pickhist,port=map(lambda df: df[['date','ticker','PortNav%']],[Ab,Ba,pickhist,port])
    Ab,Ba,pickhist,port=map(lambda df: df.sort_values(by=['PortNav%'],ascending=False),[Ab,Ba,pickhist,port])
    Ab.to_csv("D:/CompanyData/Shen7Top40_"+todayname+".csv",index=False)
    Ba.to_csv("D:/CompanyData/SectorTop15_"+todayname+".csv",index=False)
    pickhist.to_csv("D:/CompanyData/CSI300Hot_"+todayname+".csv",index=False)
    port.to_csv("D:/CompanyData/ModelPortflio_"+todayname+".csv",index=False)
    return()

def DailySecHotstock(daiyreturn,rebalday):
    rechist=HR.ActivepickBMSec(dailyreturn,'CSIAll',rebalday)
    rechist.to_csv("D:/CompanyData/Daily_SecHotstocks_"+todayname+".csv",encoding='utf-8-sig',index=False)
    return()

def Sectop_stocks(dailyreturn,rebalday):
    a,b=SS.CITICsec_analyst(dailyreturn,rebalday)
    b.to_csv("D:/CompanyData/Daily_HotCiticSector_"+todayname+".csv",encoding='utf-8-sig',index=False)
    return()

def IPO_daily(rebalday):
    IPOlastyear_sector,IPOyearhist=I.Proceeds_Analysis(rebalday)
    IPOlastyear_sector.to_csv("D:/CompanyData/IPO_TTM_"+todayname+".csv",encoding='utf-8-sig',index=False)
    IPOyearhist.to_csv("D:/CompanyData/IPO_hist_"+todayname+".csv",encoding='utf-8-sig',index=False)
    return()

def HK_Hot(rebalday):
    rechist=HR.HongKongHotStock(rebalday)
    rechist.to_csv("D:/CompanyData/HKHotStock_"+todayname+".csv",index=False)
    return()

#gentable=Combine(dailyreturn,rebalday)
#gentable2=Combine_FullUniverse(dailyreturn,rebalday)
TAcount,analyst_pick=Shen5()
CSI,CITIC=SecRDaily()
facreturnhist_Mixed=FFR.Facreturn_Mixed_daily(rebalday,dailyreturn)
facreturnhist=FFR.Facreturn_daily(dailyreturn)
portdict=Shen567(TAcount,rebalday)
SuperTrendSectorsDaily(TAcount,rebalday)
SectorValueDaily(dailyreturn)
#CompositePort(dailyreturn,rebalday)
PC.DailyModel(dailyreturn,rebalday)
DailySecHotstock(dailyreturn,rebalday)
SAA.A_ShsPrez(rebalday[0],dailyreturn)
Sectop_stocks(dailyreturn,rebalday)
HK_Hot(rebalday)