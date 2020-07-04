# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 21:41:13 2020

@author: wudi
"""

import pandas as pd
from FundaStock import Prep
from FundaStock import Funda
from Toolbox import DataStructuring 
from MSSQL import MSSQL

P=Prep()
F=Funda()
DS=DataStructuring()

def ValuationReciprocal_download(self,rebaldaylist,signal):
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    sql=[]
    for rebalday in rebaldaylist:
        sqlpart=getattr(Q,'Valuation_Reciprocal')(signal,rebalday)
        sql.append(sqlpart)
    reslist=ms.ExecListQuery(sql)
    df=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','sigvalue'])
    df['sigvalue']=df['sigvalue'].astype(float)
    df['sigvalue']=df['sigvalue'].round(5)
    return(df)

def Period_Fzdict(dailyreturn,rebaldaylist,facdict):
    selectsigs=[]
    [selectsigs.extend(v) for k, v in facdict.items()]
    siglist=list(set([x.replace('growth','') for x in selectsigs]))
    siglist=list(set([x.replace('vol','') for x in siglist]))
    facnamelist=list(facdict.keys())
    sighist=P.SigdataPrep(dailyreturn,siglist,rebaldaylist)                                #All fundadata of basic signals
    sighist=DS.GrowVol(sighist,'grow')                                                          #All growthdata of basic signals
    nsigdict=F.NSighist(dailyreturn,rebaldaylist,sighist,selectsigs)                       #Neutralize selected signals over rebaldaylist
    fzdict={}
    for facname in facnamelist:
        siginfac=facdict[facname]
        fzdict=F.Factorscore(rebaldaylist,nsigdict,facname,siginfac,fzdict)                #calculate added zscore of factor and Group it into 5Q
    return(fzdict)

def Period_facreturn(self,fztab,rebalday,returnday,dailyreturn,stock_sector):
    fztabrebal=fztab.loc[fztab['date']==rebalday,:]
    fztabrebal=fztabrebal.dropna()
    fztabrebal=fztabrebal.reset_index(drop=True)
    othorebal=DS.Othogonize(fztabrebal.drop('date',1))    
    othorebal['date']=rebalday                                                #先正交化Factor exposure
    othorebal['country']=1
    othorebal=DS.Mcap_sector(stock_sector,dailyreturn,othorebal)
    pnlrebal=DC.Period_PNL(dailyreturn,othorebal,returnday,returnday)
    othorebal=pd.merge(othorebal,pnlrebal[['ticker','dailyreturn']],on=['ticker'],how='left')
    othorebal=othorebal.dropna()
    industry_weight=pd.DataFrame(othorebal.groupby(['primecode'])['mcap'].sum())          #industry_weight: the every sector's mcap as ratio vs the last industry
    industry_weight.reset_index(inplace=True)
    industry_weight['w']=-industry_weight['mcap']/industry_weight.iloc[industry_weight.shape[0]-1,industry_weight.shape[1]-1]
    Xset=list(set(fztab.columns).difference(['date','ticker']))+['country']+sorted(set(othorebal['primecode'])) #Xset: all columns used to do the matrix operation for 因子收益calculation
    X=othorebal[Xset].copy()                                                                   #Othorebal: has everything including ticker and date, mcap....
    f=Opt.WLS_adjusted(othorebal,industry_weight,X)                                                                                
    return(f,Xset)

def Facreturn(self,startdate,dailyreturn):
    totalrebaldaylist=DC.Rebaldaylist(startdate,1)
    rebalperiodlist = [totalrebaldaylist[x:x+5] for x in range(0, len(totalrebaldaylist),5)]
    returnperiodlist= [totalrebaldaylist[x:x+5] for x in range(1, len(totalrebaldaylist),5)]
    facdict={'Quality': ['ROETTM'],'Growth': ['QRevenuegrowth'],'Value': ['PE']}
    facreturn=pd.DataFrame()
    for i in range(0,(len(rebalperiodlist)-1)):
        rebaldaylist=rebalperiodlist[i]
        returndaylist=returnperiodlist[i]
        subdict=dict(zip(rebaldaylist,returndaylist))
        fzdict=self.Period_Fzdict(dailyreturn,rebaldaylist,facdict)
        fztab=self.F.FZtab(fzdict)
        tickerlist=fztab['ticker'].unique()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CSI')
        periodfacreturn=[]
        for rebalday in rebaldaylist:
            returnday=subdict[rebalday]
            f,Xset=self.Period_facreturn(fztab,rebalday,returnday,dailyreturn,stock_sector)
            periodfacreturn.append(f)
        periodfacreturndf=pd.DataFrame(periodfacreturn,columns=Xset)
        periodfacreturndf['date']=rebaldaylist
        facreturn=facreturn.append(periodfacreturndf)
    return(facreturn)