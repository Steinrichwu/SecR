# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 15:20:56 2020

@author: wudi
"""


import pandas as pd
import numpy as np
from Toolbox import DataCollect
#from Toolbox import WeightScheme
from Toolbox import DataStructuring
from MSSQL import MSSQL
from Funda import Prep
from Funda import BTStruct 
from Toolbox import ReturnCal
from Toolbox import WeightScheme
from Consensus import Prep as ConseunsPrep
import empyrical
from HotStock import StockPick 
from HotStock import Review as HSReview

DC=DataCollect()
DS=DataStructuring()
RC=ReturnCal()
FP=Prep()
FF=BTStruct()
WS=WeightScheme()
CP=ConseunsPrep()
HSR=HSReview()
SP=StockPick()

class Prep():
    def __init__(self):
        pass
    
    #use the query in Querybase to download hitorical signal
    def Valuation_hist_daily(self,signame,tickerlist,startdate):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="Select TradingDay, SM.SecuCode, V."+signame+" from JYDBBAK.dbo.LC_DIndicesForValuation V left join JYDBBAK.dbo.SecuMain SM on V.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and SM.SecuCode in ("+str(tickerlist)[1:-1]+") and TradingDay>'"+startdate+"'"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['date','ticker',signame])
        df['date']=[str(x)[0:10]for x in df['date']]
        return(df)
    
    def Index_download(self,seccode):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sql="Select ICW.EndDate, SM.SecuCode, ICW.Weight from JYDBBAK.dbo.LC_IndexComponentsWeight ICW left join JYDBBAK.dbo.SecuMain SM on ICW.InnerCode=SM.InnerCode where ICW.IndexCode='"+seccode+"'"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['date','ticker','weight'])
        df['date']=[str(x)[0:10]for x in df['date']]
        return(df)
    
    #Extract 1y forward rolling PE. 
    def ForwardPE(self):
        forwardPE=pd.read_csv("D:/MISC/1y_forwardPE.csv")
        forwardPE=pd.melt(forwardPE,id_vars='date',value_vars=list(forwardPE.columns[1:]),var_name='ticker',value_name='1yfrdPE')
        forwardPE['ticker']=[str(x)[0:6]for x in forwardPE['ticker']]
        return(forwardPE)
    
    def Sec_weight(self,sec):
        if sec=='tech':
            seccode='8894'
        if sec=='healthcare':
            seccode='8890'
        if sec=='consumer':
            seccode='8887'
        df=self.Index_download(seccode)
        df=df.sort_values(by=['date'])
        df['weight']=df['weight']/100
        return(df)

class StupidS7():
    def __init__(self):
        self.P=Prep()
    
    def Sector_wise_index(self,seccode,rebaldaylist,dailyreturn):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
        selectsigs=[]
        [selectsigs.extend(v) for k, v in facdict.items()]
        universe=FP.Universe(dailyreturn,rebaldaylist,seccode) 
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
        fztab['count']=fztab.groupby(['date','rank'])['ticker'].transform('count')
        fztab['PortNav%']=1/fztab['count']
        PNL=[]
        for qgroup in range(1,6):
            subgroup=fztab.loc[fztab['rank']==qgroup,:].copy()
            subPNL=RC.DailyPNL(dailyreturn,subgroup)
            subPNL['qgroup']=qgroup
            PNL.append(subPNL)
        PNLdf=pd.concat(PNL)
        stocklist=fztab.loc[fztab['rank']<=2,:].copy()
        PNLdf2011=PNLdf.loc[PNLdf['date']>='2010-01-01',:].copy()
        PNLdf2016=PNLdf.loc[PNLdf['date']>='2015-12-31',:].copy()
        PNLdf2020=PNLdf.loc[PNLdf['date']>='2019-12-31',:].copy()
        PNLdf2011=self.dfcumsum(PNLdf2011)
        PNLdf2016=self.dfcumsum(PNLdf2016)
        PNLdf2020=self.dfcumsum(PNLdf2020)
        return(PNLdf2011,PNLdf2016,PNLdf2020,stocklist)
    
    def Sector_wise_index_Mcap(self,seccode,rebaldaylist,dailyreturn):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
        selectsigs=[]
        [selectsigs.extend(v) for k, v in facdict.items()]
        universe=FP.Universe(dailyreturn,rebaldaylist,seccode) 
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
        PNL=[]
        for qgroup in range(1,6):
            subgroup=fztab.loc[fztab['rank']==qgroup,:].copy()
            subgroup=WS.Generate_PortNavMcap(subgroup,dailyreturn)
            subPNL=RC.DailyPNL(dailyreturn,subgroup)
            subPNL['qgroup']=qgroup
            PNL.append(subPNL)
        PNLdf=pd.concat(PNL)
        stocklist=fztab.loc[fztab['rank']<=2,:].copy()
        PNLdf2011=PNLdf.loc[PNLdf['date']>='2010-01-01',:].copy()
        PNLdf2016=PNLdf.loc[PNLdf['date']>='2015-12-31',:].copy()
        PNLdf2020=PNLdf.loc[PNLdf['date']>='2019-12-31',:].copy()
        PNLdf2011=self.dfcumsum(PNLdf2011)
        PNLdf2016=self.dfcumsum(PNLdf2016)
        PNLdf2020=self.dfcumsum(PNLdf2020)
        return(PNLdf2011,PNLdf2016,PNLdf2020,stocklist)
    
    def ProduceQ1toQ5(self,seccode,rebaldaylist,dailyreturn):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
        selectsigs=[]
        [selectsigs.extend(v) for k, v in facdict.items()]
        universe=FP.Universe(dailyreturn,rebaldaylist,seccode) 
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
    
    def Top30tile_EqualWeight(self,seccode,rebaldaylist,dailyreturn):
        stocks=DC.Ashs_stock_seccode(rebaldaylist,[seccode],'CSI')
        mcaptab=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist))&(dailyreturn['ticker'].isin(stocks['ticker'].unique())),['date','ticker','mcap']].copy()
        mcaptab['bottom30']=mcaptab.groupby(['date'])['mcap'].transform(lambda x: x.quantile(0.6))
        mcaptab['diff']=mcaptab['mcap']-mcaptab['bottom30']
        mcaptab=mcaptab.loc[mcaptab['diff']>=0,:].copy()
        fztab=self.ProduceQ1toQ5(seccode,rebaldaylist,dailyreturn)
        fztab=fztab.loc[fztab['rank']>=3,:].copy()
        fztab['index']=fztab['date']+fztab['ticker']
        mcaptab['index']=mcaptab['date']+mcaptab['ticker']
        mcaptab=mcaptab.loc[mcaptab['index'].isin(fztab['index']),:].copy()
        mcaptab['count']=mcaptab.groupby(['date'])['ticker'].transform('count')
        mcaptab['PortNav%']=1/mcaptab['count']
        PNL=RC.DailyPNL(dailyreturn,mcaptab)
        PNL16=PNL.loc[PNL['date']>='2016-01-01',:].copy()
        PNL20=PNL.loc[PNL['date']>='2020-01-01',:].copy()
        PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        PNL16['cumReturn']=np.exp(np.log1p(PNL16['dailyreturn']).cumsum())
        PNL20['cumReturn']=np.exp(np.log1p(PNL20['dailyreturn']).cumsum())
        return(PNL,PNL16,PNL20)
    
    
    def RaiseEPS(self,seccode,rebaldaylist,dailyreturn):
        stocks=DC.Ashs_stock_seccode(rebaldaylist,[seccode],'CSI')
        stocks['index']=stocks['date']+stocks['ticker']
        Postab=CP.Consensus('Raisecon_eps',rebaldaylist)
        #Postab=Postab.loc[Postab['sigvalue']>=0,:].copy()
        Postab['index']=Postab['date']+Postab['ticker']
        CSIPostab=Postab.loc[Postab['index'].isin(stocks['index']),:].copy()
        CSIPostab['rank']=CSIPostab.groupby(['date'])["sigvalue"].rank("dense", ascending=False)
        CSIPostab['0.3count']=CSIPostab.groupby(['date'])["ticker"].transform('count')*0.3
        CSIPostab=CSIPostab.loc[CSIPostab['rank']<=CSIPostab['0.3count'],:].copy()
        CSIPostab['PortNav%']=CSIPostab.groupby(['date'])['ticker'].transform('count')
        CSIPostab['PortNav%']=1/CSIPostab['PortNav%']
        PNL=RC.DailyPNL(dailyreturn,CSIPostab)
        PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        return(CSIPostab,PNL)
    
    def Intersect_weighting(self,df,benchmark):
        dfsect=WS.Benchmark_intersect(df,benchmark)
        dfsect['weight']=dfsect['weight']/100
        dfsect['totalweight%']=dfsect.groupby('date')['weight'].transform('sum')
        dfsect['PortNav%']=dfsect['weight']/dfsect['totalweight%']
        dfsect=dfsect[['date','ticker','PortNav%']]
        return(dfsect)
    
    #Compare the change of recommendations vs the total # of recommendations 
    def HotStock_Transit(self,rebaldaylist,dailyreturn):
        activepickhist=HSR.ActivepickNS_production(dailyreturn,rebaldaylist,150)
        activepickhist=activepickhist.sort_values(by=['ticker','date'],ascending=[True,True])
        activepickhist['rec_chg']=activepickhist['raccount']-activepickhist['raccount'].shift(1)
        activepickhist['nthoccur']=activepickhist.groupby('ticker').cumcount()+1
        activepickhist.loc[activepickhist['nthoccur']==1,'rec_chg']=0
        activepickhist['rec_chg_rank']=activepickhist.groupby(['date'])['rec_chg'].rank("dense", ascending=False)
        newhotstock=activepickhist.loc[activepickhist['rec_chg_rank']<=10,:].copy()
        #newhotstock=self.Intersect_weighting(newhotstock,'CSI300')
        newhotstock['count']=newhotstock.groupby(['date'])['ticker'].transform('count')
        newhotstock['PortNav%']=1/newhotstock['count']
        PNL=RC.DailyPNL(dailyreturn,newhotstock)
        PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        activepickhist['rank']=activepickhist.groupby(['date'])['raccount'].rank("dense", ascending=False)
        oldhotstock=activepickhist.loc[activepickhist['rank']<=70,:].copy()
        oldhotstock['count']=oldhotstock.groupby(['date'])['ticker'].transform('count')
        oldhotstock['PortNav%']=1/oldhotstock['count']
        PNL2=RC.DailyPNL(dailyreturn,oldhotstock)
        PNL2['cumReturn']=np.exp(np.log1p(PNL2['dailyreturn']).cumsum())
        return(PNL,PNL2,newhotstock)
    
    #超级趋势自定义指数
    #Generate the Benchmark index return.
    #Choose the top 60% marketcap stocks, and retain Q3-Q5 shen7 stocks
    #MarketCap weighted with a cap at 15% 
    def Top30tile_MCapCap(self,seccode,rebaldaylist,dailyreturn):
        stocks=DC.Ashs_stock_seccode(rebaldaylist,[seccode],'CSI')
        mcaptab=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist))&(dailyreturn['ticker'].isin(stocks['ticker'].unique())),['date','ticker','mcap']].copy()
        mcaptab['bottom30']=mcaptab.groupby(['date'])['mcap'].transform(lambda x: x.quantile(0.4))
        mcaptab['diff']=mcaptab['mcap']-mcaptab['bottom30']
        mcaptab=mcaptab.loc[mcaptab['diff']>=0,:].copy()
        fztab=self.ProduceQ1toQ5(seccode,rebaldaylist,dailyreturn)
        fztab=fztab.loc[fztab['rank']>=3,:].copy()
        fztab['index']=fztab['date']+fztab['ticker']
        mcaptab['index']=mcaptab['date']+mcaptab['ticker']
        mcaptab=mcaptab.loc[mcaptab['index'].isin(fztab['index']),:].copy()
        mcaptab['totalMcap']=mcaptab.groupby(['date'])['mcap'].transform('sum')
        mcaptab['PortNav%']=mcaptab['mcap']/mcaptab['totalMcap']
        while (mcaptab['PortNav%'].max()>0.15):
            mcaptab=self.Generate_PortNavMcap_CAP(mcaptab,dailyreturn)
        PNL=RC.DailyPNL(dailyreturn,mcaptab)
        PNL16=PNL.loc[PNL['date']>='2016-01-01',:].copy()
        PNL20=PNL.loc[PNL['date']>='2020-01-01',:].copy()
        PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        PNL16['cumReturn']=np.exp(np.log1p(PNL16['dailyreturn']).cumsum())
        PNL20['cumReturn']=np.exp(np.log1p(PNL20['dailyreturn']).cumsum())
        return(PNL,PNL16,PNL20,mcaptab)
    
    def Generate_PortNavMcap_CAP(self,df2,dailyreturn):
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
    
    def dfcumsum(self,PNLdf):
        PNLdf['cumreturn']=np.log1p(PNLdf['dailyreturn'])
        PNLdf['cumreturn']=PNLdf.groupby(['qgroup'])['cumreturn'].transform("cumsum")
        PNLdf['cumreturn']=np.exp(PNLdf['cumreturn'])
        PNLdf=PNLdf.pivot_table(index='date',columns='qgroup',values='cumreturn',aggfunc='first')
        PNLdf.reset_index(inplace=True)
        return(PNLdf)
    
        #pnl=fztab.groupby(['rank'])['']
        #stocklist=fztab.loc[fztab['rank']==5,:].copy()
        #PNL=self.EqPNL(stocklist,dailyreturn)
        #PNL=PNL.rename(columns={'dailyreturn':'dailyPNLQ5'})
        #PNL=PNL.rename(columns={'cumPNL':'cumPNLQ5'})
        #stocklist2=fztab.loc[fztab['rank']==1,:].copy()
        #PNL2=self.EqPNL(stocklist2,dailyreturn)
        #PNL2=PNL2.rename(columns={'dailyreturn':'dailyPNLQ1'})
        #PNL2=PNL2.rename(columns={'cumPNL':'cumPNLQ1'})
        #PNL=pd.merge(PNL,PNL2,on='date',how='left')
        #return(PNL,stocklist)
    
    def EqPNL(self,stocklist,dailyreturn):
        stocklist['count']=stocklist.groupby(['date'])['ticker'].transform('count')
        stocklist['PortNav%']=1/stocklist['count']
        PNL=RC.DailyPNL(dailyreturn,stocklist)
        PNL['cumPNL']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        return(PNL)
    

class Analysis():
    def __init__(self):
        self.P=Prep()
    
    #given the table of date,ticker and weight, calculate ForwardPE_median
    def ForwardPE_median(self,memb):
        tickerlist=list(memb['ticker'].unique())
        PE=self.P.ForwardPE()
        PE=PE.loc[PE['ticker'].isin(tickerlist)]
        rebaldaylist=list(PE.loc[PE['date']>='2020-01-01','date'].unique())
        his_weight=DS.Rebalday_alignment(memb,rebaldaylist)
        PE['index']=PE['date']+PE['ticker']
        his_weight['index']=his_weight['date']+his_weight['ticker']
        his_weight=pd.merge(his_weight,PE[['index','1yfrdPE']],on='index',how='left')
        his_weight['1yfrdPE']=his_weight['1yfrdPE'].astype(float)
        #his_weight['PE']=his_weight.groupby(['date'])['PE'].transform(lambda x: mstats.winsorize(x,limits=[0.05,0.05],axis=0,inplace=True))
        #his_weight['weightedPE']=his_weight['weight']*his_weight['PE']
        #his_weight['secPE']=his_weight.groupby(['date'])['weightedPE'].transform(sum)
        his_weight['medianfrdPE']=his_weight.groupby(['date'])['1yfrdPE'].transform('median')
        his_weight=his_weight.drop_duplicates(subset='date')
        his_weight=his_weight[['date','medianfrdPE']]
        #PNL=RC.DailyPNL(dailyreturn,his_weight)
        #PNL['cumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        return(his_weight)
    
    
    def ForwardPE_and_Return(self,sec,dailyreturn):
        memb=self.P.Sec_weight(sec)
        memb['date']=[str(x)[0:10]for x in memb['date']]
        memb=memb.rename(columns={'weight':'PortNav%'})
        memb['PortNav%']=memb['PortNav%'].astype(float)
        memb=memb.loc[memb['date']>='2019-12-30',:].copy()
        IndexFwdPEmedian=self.ForwardPE_median(memb)
        portmemb=pd.read_csv("D:/S/SuperTrend/"+sec+"_list.csv")
        portmemb['date']='2019-12-30'
        portmemb['ticker']=[str(x)[0:6]for x in portmemb['ticker']]
        portmemb['PortNav%']=1/len(portmemb)
        PortFwdPEmedian=self.ForwardPE_median(portmemb)
        PortFwdPEmedian=PortFwdPEmedian.rename(columns={'medianfrdPE':'Port1yfrdPE'})
        IndexFwdPEmedian=IndexFwdPEmedian.rename(columns={'medianfrdPE':'Index1yfrdPE'})
        IndexFwdPEmedian=pd.merge(IndexFwdPEmedian,PortFwdPEmedian,on='date',how='left')
        IndexReturn=RC.DailyPNL(dailyreturn,memb)
        PortReturn=RC.DailyPNL(dailyreturn,portmemb)
        PortReturn['PortReturn']=np.exp(np.log1p(PortReturn['dailyreturn']).cumsum())
        IndexReturn['IndexReturn']=np.exp(np.log1p(IndexReturn['dailyreturn']).cumsum())
        PortReturn=PortReturn.rename(columns={'dailyreturn':'PortdailyReturn'})
        IndexReturn=IndexReturn.rename(columns={'dailyreturn':'IndexdailyReturn'})
        IndexReturn=pd.merge(IndexReturn[['date','IndexdailyReturn','IndexReturn']],PortReturn[['date','PortdailyReturn','PortReturn']],on='date',how='left')
        IndexReturn['cumAlpha']=IndexReturn['PortReturn']-IndexReturn['IndexReturn']
        IndexFwdPEmedian['ValuationGap']=IndexFwdPEmedian['Port1yfrdPE']/IndexFwdPEmedian['Index1yfrdPE']
        #PEGap_Return=pd.merge(IndexFwdPEmedian[['date','ValuationGap']],IndexReturn[['date','PortdailyReturn','IndexdailyReturn','IndexReturn','PortReturn','cumAlpha']],on='date',how='left')
        #PEGap_Return.set_index(['date'],inplace=True)
        #PEGap_Return[['ValuationGap','cumAlpha']].plot()
        print('max drawdown index'+ str(empyrical.max_drawdown(IndexReturn['IndexdailyReturn'])))
        print('sharpe index'+ str(empyrical.sharpe_ratio(IndexReturn['IndexdailyReturn'])))
        print('max drawdown port'+ str(empyrical.max_drawdown(IndexReturn['PortdailyReturn'])))
        print('sharpe port'+ str(empyrical.sharpe_ratio(IndexReturn['PortdailyReturn'])))
        return(IndexReturn)

    def Tilestats2(self):
        memb=pd.read_csv("D:/MISC/CJQS_Healthcare.csv")
        memb['ticker']=[str(x)[0:6]for x in memb['ticker']]
        memb['date']='2020-01-02'
        tickerlist=list(memb['ticker'].unique())
        PE=self.P.ForwardPE()
        PE=PE.loc[PE['ticker'].isin(tickerlist)]
        rebaldaylist=list(PE.loc[PE['date']>='2020-01-01','date'].unique())
        his_weight=DS.Rebalday_alignment(memb,rebaldaylist)
        PE['index']=PE['date']+PE['ticker']
        his_weight['index']=his_weight['date']+his_weight['ticker']
        his_weight=pd.merge(his_weight,PE[['index','1yfrdPE']],on='index',how='left')
        his_weight['1yfrdPE']=his_weight['1yfrdPE'].astype(float)
        #his_weight['PE']=his_weight.groupby(['date'])['PE'].transform(lambda x: mstats.winsorize(x,limits=[0.05,0.05],axis=0,inplace=True))
        #his_weight['weightedPE']=his_weight['weight']*his_weight['PE']
        #his_weight['secPE']=his_weight.groupby(['date'])['weightedPE'].transform(sum)
        his_weight['medianPE']=his_weight.groupby(['date'])['1yfrdPE'].transform('median')
        his_weight=his_weight.drop_duplicates(subset='date')
        return(his_weight)

    