# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 20:18:08 2020

@author: wudi
"""
import pandas as pd
import numpy as np
from datetime import date
from Toolbox import DataCollect 
from Toolbox import WeightScheme
from Toolbox import DataStructuring
from Toolbox import ReturnCal
from Funda import Review 
from Funda import BTStruct 
from Funda import Prep
from HotStock import StockPick 
from HotStock import Review as HSReview
from functools import reduce
from Consensus import Prep as ConsensusPrep

FF=BTStruct()
FR=Review()
FP=Prep()
DC=DataCollect()
WS=WeightScheme()
RC=ReturnCal()
DS=DataStructuring()
SP=StockPick()
HSR=HSReview()
CP=ConsensusPrep()
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

#mergedPosNew=self.SimpMerged(dailyreturn,mergedPos)
#mergedPNLNew=self.BT.GenPNL(mergedPosNew,dailyreturn)
#PNLreview=self.Alpha(mergedPNLNew)

###########################################################################################
#PartA- Cross sector Shen56 Based Quantamental: 
#       Top40 stocks based on Shen56 universe; no need to take top 70% market cap of the universe
#
#PartB- BySector Marketbased Quantamental:
#   PartB1:Top10 stocks of each sector using sector specific quantamental ratios defined in BySectorFacdict, Bank, NonBank and Property use top7 of hotstock
#   PartB2:Each sector's top10 stocks marketcap weighted.
#   PartB3:between sectors, use the latest avaialble CSI sector weights, Banks/NonBanks/Properties need to be restructured
#   PartB4:Retain only 60 stocks, and pro-rata expand the 60% weighting to 100%
#
#PartC- Consensus:
#   PartC1:Hotstock, the 70 most recommended stocks in the past 60 days, Intersect with CSI300, CSI mirror weighted
#   PartC2:Raise_con_eps model: The top40 stocks that have highest eps raises in the past rebal period, equal weighted
#   PartC1-C2: 50%-50% to construct Consensus portfolio

#PartD- Shen56:
#   Shen56 of every period, CSI300-intersect weighted
#
#
#Portfolio Construction:
#   30% to Part-A, 15% to Part-B, 30% to PartC, 25% to Shen56 
#   Keep 100 stocks and prorata expand the weights to 100%
#
#Exeuction:
#   PC=PortConst()
#   mergedPosNew,mergedPNLNew=PC.Execution()
###########################################################################################
class Structuring():
    def __init__(self):
        pass
    
    #Input: each rebalday a portfolio of every sector's top10, every sector's full NAV is 100% on rebalday
    #Output: each rebalday a portfolio of 100% weight, CSI300 sector weights allocated
    def Sec_weightPort(self,historical_allsecpos):
        allsecweighted=WS.MixedSecWeight(historical_allsecpos)
        allsecweighted['PortNav%']=allsecweighted['PortNav%']*allsecweighted['secweight']
        return(allsecweighted)
    
    #Input: a portfolio consisting of top stocks in every sector, with rebal day's weight across all sectors=100%
    #Output: shrink the portfolio above to 60 names only, rebal day's total weight=100%
    def Sec_simpweightPort(self,allsecweighted):
        allsecposTop60=allsecweighted.groupby('date').apply(lambda x: x.nlargest(100,'PortNav%')).reset_index(drop=True)
        allsecposTop60['totalweight']=allsecposTop60.groupby('date')['PortNav%'].transform('sum')
        allsecposTop60['PortNav%']=allsecposTop60['PortNav%']/allsecposTop60['totalweight']
        return(allsecposTop60)
    
    #Merge the PortNav% of same names of different portfolio; ecah portofio has 100% total sum of PortNav% on rebal day
    def Merge_port(self,appended_port):
        appended_port['index']=appended_port['date']+appended_port['ticker']
        appended_port['PortNav%']=appended_port.groupby('index')['PortNav%'].transform('sum')
        appended_port=appended_port.drop_duplicates()
        merged_port=appended_port.copy()
        merged_port['sumNav']=merged_port.groupby('date')['PortNav%'].transform('sum')
        merged_port['PortNav%']=merged_port['PortNav%']/merged_port['sumNav']
        merged_port=merged_port.drop(['index','sumNav'],axis=1)
        return(merged_port)
    
    def Intersect_weighting(self,df,benchmark):
        dfsect=WS.Benchmark_intersect(df,benchmark)
        dfsect['weight']=dfsect['weight']/100
        dfsect['totalweight%']=dfsect.groupby('date')['weight'].transform('sum')
        dfsect['PortNav%']=dfsect['weight']/dfsect['totalweight%']
        dfsect=dfsect[['date','ticker','PortNav%']]
        return(dfsect)
    
class ROE_PE():
    def __init__(self):
        self.BST=Structuring()
    
    def ROE_PE_port(self,dailyreturn,rebaldaylist):
        universecode='Market'
        universe=FP.Universe(dailyreturn,rebaldaylist,universecode)
        sighist=FP.SigdataPrep(dailyreturn,['ROETTM','PE','OpRevGrowthYOY'],rebaldaylist)  
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
           ROEselect=ROE.nlargest(int(stockcount*0.1),'sigvalue',keep='all')
           ROEselectticker=list(ROEselect.loc[ROEselect['sigvalue']>0,'ticker'].unique())
           stockcount=len(ROEselectticker)
           Value=rebal_sighist.loc[(rebal_sighist['signame']=='PE')&(rebal_sighist['ticker'].isin(ROEselectticker)),:].copy()
           Valueselect=Value.nlargest(int(stockcount*0.2),'sigvalue',keep='all')
           selectdf=pd.DataFrame(data={'date':rebalday,'ticker':Valueselect['ticker']})
           port=port.append(selectdf)
        port['memb']=port.groupby('date')['ticker'].transform('count')           #Take intersect, but use equal weight
        port['PortNav%']=1/port['memb']
        port=port.drop(['memb'],axis=1)
        return(port)
    
        
class BySectorPort():
    def __init__(self):
        pass
    
    #Get the list of all signals in the nested list
    def NestedDictValues(self,d):
        keys=list(d.keys())
        siglist=[]
        for key in keys:
            subkeys=list(d[key].keys())
            for subkey in subkeys:
                sig=d.get(key,{}).get(subkey)
                siglist.extend(sig)
        siglist=siglist=list(set(siglist))
        return(siglist)
    
    #The top selected stocks for Financial industry
    def FinancePC(self,dailyreturn,rebaldaylist):
        FinanceStats=SP.Rec_stat(dailyreturn,rebaldaylist,60,['40','41','42'],'CITIC')
        FinanceStats=DC.Sector_get(FinanceStats,'CITIC')
        FinanceStats=DC.Second_sector_get(FinanceStats)
        FinanceStats.loc[FinanceStats['primecode']=='41','primecode']=FinanceStats.loc[FinanceStats['primecode']=='41','secondcode']
        FinanceStats['index']=FinanceStats['date']+FinanceStats['primecode']
        FinanceStats['raccount']=FinanceStats['raccount'].astype(int)
        financetop=FinanceStats.groupby('index').apply(lambda x: x.nlargest(4,'raccount')).reset_index(drop=True)
        financetop=financetop[['date','ticker','primecode']]
        return(financetop)
    
    #Everysector has its own signals
    def BySectorFacdict(self):
        dicfacdict={}
        dicfacdict['00']={'Quality': ['CashRateOfSales'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        dicfacdict['01']={'Quality': ['ROE','ROA','CashRateOfSales'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PB','PS'],'Market':['turnoverweek']}
        dicfacdict['02']={'Quality': ['ROE','ROA'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PB','PS','PE'],'Market':['turnoverweek']}
        dicfacdict['03']={'Quality': ['ROE','ROA'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PB','PS','PE'],'Market':['turnoverweek']}
        dicfacdict['04']={'Quality': ['OperatingRevenuePS',	'OperCashInToAsset','TotalAssetTRate'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        dicfacdict['05']={'Quality': ['OperatingRevenuePS'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        #dicfacdict['06']={'Quality': ['OperatingRevenuePS','WorkingCapital'],'Growth': ['QNetprofitgrowth'],'Value': ['PB','PE'],'Market':['turnoverweek']}
        dicfacdict['07']={'Quality': ['ROE'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        dicfacdict['08']={'Quality': ['ROE'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PB'],'Market':['turnoverweek']}
        dicfacdict['09']={'Quality': ['ROA'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PB','PE'],'Market':['turnoverweek']}
        selectsigs=self.NestedDictValues(dicfacdict)
        return(dicfacdict,selectsigs)
    
    #Structure the fundmantal meanscore of each sectore, return the historical meanscore/factor zscore tab 
    def BySectorStructure(self,dailyreturn,rebaldaylist,universecode,dicfacdict,sighist):
        sec_selecsigs=[]
        [sec_selecsigs.extend(v) for k, v in dicfacdict[universecode].items()]
        universe=FP.Universe(dailyreturn,rebaldaylist,universecode) 
        nsigdict=FF.NSighist(dailyreturn,rebaldaylist,sighist,sec_selecsigs,universe)
        facnamelist=list(dicfacdict[universecode].keys())
        facinfacz=[x+'_zscore' for x in facnamelist]
        fzdict={}
        for facname in facnamelist:
            siginfac=dicfacdict[universecode][facname]
            fzdict=FF.Factorscore(rebaldaylist,nsigdict,facname,siginfac,fzdict) 
        fztab=FF.FZtab(fzdict)
        fztab['narow']=fztab.isnull().sum(axis=1)
        #fztab=fztab.loc[fztab['narow']<=1,:].copy()       #cannot have more than 1 na iterms, 1na is fine
        fztab['meanscore']=np.mean(fztab[facinfacz],axis=1)
        return(fztab)
    
    #Construct by Sector fundamental-based Portfolios, each sector on each rebal day has 100%, finance use hotstocks
    def BySectorPC(self,dailyreturn,rebaldaylist):
        financetop=self.FinancePC(dailyreturn,rebaldaylist)
        dicfacdict,selectsigs=self.BySectorFacdict()
        siglist=list(set([x.replace('growth','') for x in selectsigs]))
        siglist=list(set([x.replace('vol','') for x in siglist]))
        sighist=FP.SigdataPrep(dailyreturn,siglist,rebaldaylist)     
        sighist=DS.GrowVol(sighist,'grow')
        sectorlist=list(dicfacdict.keys())
        sec_top10=pd.DataFrame(columns=['date','ticker','meanscore','primecode'])
        for universecode in sectorlist:
            fztab=self.BySectorStructure(dailyreturn,rebaldaylist,universecode,dicfacdict,sighist)
            top10=fztab.groupby('date').apply(lambda x: x.nlargest(10,'meanscore')).reset_index(drop=True)
            top10['primecode']=universecode
            top10=top10[['date','ticker','primecode']]
            sec_top10=sec_top10.append(top10)
        sec_top10=sec_top10.append(financetop)
        historical_allsecpos=WS.Generate_PortNavMcap_allsecs(sec_top10,dailyreturn)   #Each sector's total mcap on each rebalday is 100%
        return(historical_allsecpos)
    

class Backtest():
    def __init__(self):
        self.BSP=BySectorPort()
        self.BST=Structuring()
    
    def GenPNL(self,df,dailyreturn):
        dfPNL=RC.DailyPNL(dailyreturn,df)
        dfPNL['cumreturn']=np.exp(np.log1p(dfPNL['dailyreturn']).cumsum())
        return(dfPNL)
    
    #Backtest sec_weighted portfolio return (non simpweighted)
    def BySectorBT(self,dailyreturn,rebaldaylist):
        historical_allsecpos=self.BSP.BySectorPC(dailyreturn,rebaldaylist)
        allsecweighted=self.BST.Sec_weightPort(historical_allsecpos)
        return(allsecweighted)
    
    #Backtest sec_weighted portfolio return (simpweighted)
    def BySectorSimpBT(self,dailyreturn,rebaldaylist):
        historical_allsecpos=self.BSP.BySectorPC(dailyreturn,rebaldaylist)
        allsecweighted=self.BST.Sec_weightPort(historical_allsecpos)
        allsecSimpweighted=self.BST.Sec_simpweightPort(allsecweighted)
        return(allsecSimpweighted)
    
    
    #Backtest single factor's return 
    #facdict={'Consensus':['Raisecon_or']},ranktype='5Q',universecode='Market'/'00'
    def CrossSectorBT(self,dailyreturn,rebaldaylist):
        Postab=CP.Consensus('Raisecon_eps',rebaldaylist)
        Postab=Postab.groupby('date').apply(lambda x: x.nlargest(100,'sigvalue')).reset_index(drop=True)
        Postab=self.BST.Intersect_weighting(Postab,'CSI800')
        #Postab['memb']=Postab.groupby('date')['ticker'].transform('count')     
        #Postab['PortNav%']=1/Postab['memb']
        #Postab=Postab.drop(['memb'],axis=1)
        Postab=Postab[['date','ticker','PortNav%']]
        return(Postab)
    
    #Input:historical postab, Output: intersect with CSI300 and take equal weight on stocks 
    def CSI300Inter_Reweight(self,dailyreturn,Postab,weightscheme):
        CSIPostab=WS.Benchmark_intersect(Postab,'CSI300')
        if weightscheme=='Equal':
            CSIPostab=WS.Generate_PortNavEqual(CSIPostab)
        elif weightscheme=='MCap':
            CSIPostab=WS.Generate_PortNavMcap(CSIPostab)
        elif weightscheme=='Intersect':
            CSIPostab=WS.Generate_PortNav(CSIPostab)
        CSIPNLtab=RC.DailyPNL(dailyreturn,CSIPostab)
        CSIPNLtab['cumreturn']=np.exp(np.log1p(CSIPNLtab['dailyreturn']).cumsum())
        return(CSIPostab,CSIPNLtab)

class Risk():
    def __init__(self):
        self.BSP=BySectorPort()
        self.BST=Structuring()
        self.BT=Backtest()
    
    def Risk_vsBM(self,df):
        rebaldaylist=list(df['date'].unique())
        membhist=DC.Benchmark_membs('CSI300','2020-01-01')
        benchmark=DS.Rebalday_alignment(membhist,rebaldaylist)
        startdate=rebaldaylist[0]
        ROE=FP.Funda_download(startdate,'ROE')
        OpRevGrowth=FP.Funda_download(startdate,'OpRevGrowthYOY')
        OpRevGrowth3Y=FP.Funda_download(startdate,'OpRevGrowth3Y')
        ROE,OpRevGrowth,OpRevGrowth3Y=map(DS.Get_lastrecord,(ROE,OpRevGrowth,OpRevGrowth3Y))
        ROE=ROE.rename(columns={'sigvalue':'ROE'})
        OpRevGrowth=OpRevGrowth.rename(columns={'sigvalue':'OpRevGrowth'})
        OpRevGrowth3Y=OpRevGrowth3Y.rename(columns={'sigvalue':'OpRevGrowth3Y'})
        data_frame=[df,ROE,OpRevGrowth,OpRevGrowth3Y]
        data_frame2=[benchmark,ROE,OpRevGrowth,OpRevGrowth3Y]
        dfmerged=reduce(lambda left,right: pd.merge(left,right, on=['ticker'],how='left'),data_frame)
        bmmerged=reduce(lambda left,right: pd.merge(left,right, on=['ticker'],how='left'),data_frame2)
        PortROE=(dfmerged['PortNav%']*dfmerged['ROE']).sum()
        CSIROE=(bmmerged['weight']*bmmerged['ROE']).sum()/100
        PortGrowth=(dfmerged['PortNav%']*dfmerged['OpRevGrowth']).sum()
        CSIGrowth=(bmmerged['weight']*bmmerged['OpRevGrowth']).sum()/100
        Comptable=pd.DataFrame(columns=['metrics','stats'])
        Comptable['metrics']=['PortROE','CSIROE','PortGrowth','CSIGrowth']
        Comptable['stats']=[PortROE,CSIROE,PortGrowth,CSIGrowth]
        return(Comptable)
    
    def BrokerSector_overweight(self,df):
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
    
    
    def Sector_constraint(self,df):
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
    
    
    #Get the sector concentration of historical position tab (df) vs CSI300, and apply sector_constraint on it
    def RiskMgmt(self,df):
        dfsec=WS.MixedSecWeight(df)                #download the new primecode and sector weight (using mixed sector 银行保险券商分开)
        dfsec=dfsec.rename(columns={'secweight':'bm_secweight'})
        dfsec['secweight']=dfsec.groupby(['date','primecode'])['PortNav%'].transform('sum')
        dfsec=self.BrokerSector_overweight(dfsec)
        df=self.Sector_constraint(dfsec)
        return(df)
    
class PortConst():
    def __init__(self):
        self.BSP=BySectorPort()
        self.BST=Structuring()
        self.BT=Backtest()
        self.RP=ROE_PE()
        self.Risk=Risk()
    
    #Use one set of criteria to choose top40 from Shen56 set
    #Output: historical top 40 of date, ticker and PortNAV% and historical P&L
    def PartA_Shen7Top40(self,dailyreturn,rebaldaylist):
        facdict={'Quality': ['ROETTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        portdict,fzdict,fztab=FR.IntegratedBT(dailyreturn,rebaldaylist,facdict,'Top100','Shen56')
        #Markettop40Pos=fztab.groupby('date').apply(lambda x: x.nlargest(40,'meanscore')).reset_index(drop=True)
        #MarkettopPos=WS.Generate_PortNavEqual(Markettop40Pos)
        Markettop=fztab.groupby('date').apply(lambda x: x.nlargest(100,'meanscore')).reset_index(drop=True) #100 for the presentation
        MarkettopPos=self.BST.Intersect_weighting(Markettop,'CSI800')
        MarkettopPos['memb']=MarkettopPos.groupby('date')['ticker'].transform('count')           #Take intersect, but use equal weight
        MarkettopPos['PortNav%']=1/MarkettopPos['memb']
        MarkettopPos=MarkettopPos.drop(['memb'],axis=1)
        return(MarkettopPos)
        
    def PartB_BySector(self,dailyreturn,rebaldaylist):
        BySectorSimPos=self.BT.BySectorSimpBT(dailyreturn,rebaldaylist)
        BySectorSimPos=BySectorSimPos[['date','ticker','PortNav%']]
        return(BySectorSimPos)
    
    def PartC_Consensus(self,dailyreturn,rebaldaylist):
        activepickhist=HSR.ActivepickNS_production(dailyreturn,rebaldaylist,100)
        HotPos=self.BST.Intersect_weighting(activepickhist,'CSI300')
        ConPostab=self.BT.CrossSectorBT(dailyreturn,rebaldaylist)
        appended_port=HotPos.append(ConPostab)
        ConsensusPos=self.BST.Merge_port(appended_port)
        ConsensusPos=ConsensusPos.sort_values(by=['date'],ascending=[True])
        return(ConsensusPos)
    
    def PartD_Shen56CSI(self,dailyreturn,rebaldaylist):
        #universe=pd.read_csv("D:/SecR/Shen56New.csv")
        universe=pd.read_csv("D:/SecR/NewThreeFour_20200802.csv") #This generate higher return in YTD backtest
        universe['ticker']=[str(x).zfill(6)for x in universe['ticker']]
        universe=DS.Rebalday_alignment(universe,rebaldaylist)
        Shen56CSIpos=self.BST.Intersect_weighting(universe,'CSI300')
        return(Shen56CSIpos)
    
    def PartD_Shen56CSIDaily(self,dailyreturn,rebaldaylist):
        TAcount=pd.read_csv("D:/SecR/ThreeFourlatest.csv")
        TAcount['ticker']=[str(x).zfill(6)for x in TAcount['ticker']]
        universe1=list(TAcount['ticker'].unique())
        universe2=pd.read_csv("D:/SecR/DailyShen6.csv")
        universe2['ticker']=[str(x).zfill(6)for x in universe2['ticker']]
        universe1.extend(universe2['ticker'])
        universe=pd.DataFrame(list(set(universe1)),columns=['ticker'])
        universe['date']=rebaldaylist[0]
        Shen56CSIpos=self.BST.Intersect_weighting(universe,'CSI300')
        return(Shen56CSIpos)
    
    def PartA_Shen7TopDaily(self,dailyreturn,rebaldaylist):
        facdict={'Quality': ['ROETTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE'],'Market':['turnoverweek']}
        portdict,fzdict,fztab=FR.IntegratedBT(dailyreturn,rebaldaylist,facdict,'Top100','Shen56latest')
        #Markettop40Pos=fztab.groupby('date').apply(lambda x: x.nlargest(40,'meanscore')).reset_index(drop=True)
        #MarkettopPos=WS.Generate_PortNavEqual(Markettop40Pos)
        Markettop=fztab.groupby('date').apply(lambda x: x.nlargest(100,'meanscore')).reset_index(drop=True) #100 for the presentation
        MarkettopPos=self.BST.Intersect_weighting(Markettop,'CSI800')
        MarkettopPos['memb']=MarkettopPos.groupby('date')['ticker'].transform('count')           #Take intersect, but use equal weight
        MarkettopPos['PortNav%']=1/MarkettopPos['memb']
        MarkettopPos=MarkettopPos.drop(['memb'],axis=1)
        return(MarkettopPos)
    
    def PartE_RP(self,dailyreturn,rebaldaylist):
        RPport=self.RP.ROE_PE_port(dailyreturn,rebaldaylist)
        return(RPport)
    
    def PortBuild_Agressive(self,dailyreturn,rebaldaylist):
        Markettop40Pos=self.PartA_Shen7Top40(dailyreturn,rebaldaylist)
        #BySectorSimPos=self.PartB_BySector(dailyreturn,rebaldaylist)
        ConsensusPos=self.PartC_Consensus(dailyreturn,rebaldaylist)
        Shen56CSIPos=self.PartD_Shen56CSI(dailyreturn,rebaldaylist)
        #RPPos=self.PartE_RP(dailyreturn,rebaldaylist)
        Markettop40Pos['PortNav%']=Markettop40Pos['PortNav%']*0.3 #0.3 (for presentation backtest)
        #BySectorSimPos['PortNav%']=BySectorSimPos['PortNav%']*0.15 #0.15 (for presentation backtest)
        ConsensusPos['PortNav%']=ConsensusPos['PortNav%']*0.3 #0.3 (for presentation backtest)
        Shen56CSIPos['PortNav%']=Shen56CSIPos['PortNav%']*0.4 #0.25 (for presentation backtest)
        #RPPos['PortNav%']=RPPos['PortNav%']*0.15 #Not allocated in presentation backtest
        #appended_port=Markettop40Pos.append([ConsensusPos,Shen56CSIPos,RPPos])
        appended_port=Markettop40Pos.append([ConsensusPos,Shen56CSIPos])
        mergedPos=self.BST.Merge_port(appended_port)
        mergedPosNew=self.SimpMerged(dailyreturn,mergedPos)
        mergedPosNew=self.Risk.RiskMgmt(mergedPosNew)
        return(mergedPosNew)
    
    def PortBuild_Moderate(self,dailyreturn,rebaldaylist):
        Markettop40Pos=self.PartA_Shen7Top40(dailyreturn,rebaldaylist)
        ConsensusPos=self.PartC_Consensus(dailyreturn,rebaldaylist)
        Shen56CSIPos=self.PartD_Shen56CSI(dailyreturn,rebaldaylist)
        RPPos=self.PartE_RP(dailyreturn,rebaldaylist)
        Markettop40Pos['PortNav%']=Markettop40Pos['PortNav%']*0.3 #0.3 (for presentation backtest)
        ConsensusPos['PortNav%']=ConsensusPos['PortNav%']*0.3 #0.3 (for presentation backtest)
        Shen56CSIPos['PortNav%']=Shen56CSIPos['PortNav%']*0.25 #0.25 (for presentation backtest)
        RPPos['PortNav%']=RPPos['PortNav%']*0.15 #Not allocated in presentation backtest
        appended_port=Markettop40Pos.append([ConsensusPos,Shen56CSIPos,RPPos])
        mergedPos=self.BST.Merge_port(appended_port)
        mergedPosNew=self.SimpMerged(dailyreturn,mergedPos)
        mergedPosNew=self.Risk.RiskMgmt(mergedPosNew)
        return(mergedPosNew)
    
    def SimpMerged(self,dailyreturn,mergedPos):
        mergedPosNew=mergedPos.groupby('date').apply(lambda x: x.nlargest(100,'PortNav%',keep='all')).reset_index(drop=True)
        mergedPosNew['totalweight']=mergedPosNew.groupby('date')['PortNav%'].transform('sum')
        mergedPosNew['PortNav%']=mergedPosNew['PortNav%']/mergedPosNew['totalweight']
        mergedPosNew=mergedPosNew.drop(['totalweight'],axis=1)
        return(mergedPosNew)
    
    def Alpha(self,mergedPNL):
        PNL=mergedPNL.copy()
        PNL['stratReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        benchmarkreturn=DC.Benchmark_return('CSI300',PNL['date'].min())
        benchmarkreturn['bmdailyreturn']=benchmarkreturn['bmdailyreturn']/100
        PNL=pd.merge(PNL,benchmarkreturn,on='date',how='left')
        PNL['benchmarkReturn']=np.exp(np.log1p(PNL['bmdailyreturn']).cumsum())
        PNL['dailyAlpha']=PNL['dailyreturn']-PNL['bmdailyreturn']
        PNL['cumAlpha']=np.exp(np.log1p(PNL['dailyAlpha']).cumsum())
        PNLplot=PNL.copy()
        PNLplot.set_index(['date'],inplace=True)
        PNLplot[['cumAlpha','stratReturn','benchmarkReturn']].plot()
        return(PNL)
    
    #One Stop Backtest Execution.
    def Execution(self):
        startdate='2019-12-30'
        #startdate='2009-12-30'
        rebal_period=5
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        dailyreturn=DC.Dailyreturn_retrieve()
        mergedPos=self.PortBuild_Agressive(dailyreturn,rebaldaylist)
        mergedPos=self.SimpMerged(dailyreturn,mergedPos)
        mergedPos=self.Risk.RiskMgmt(mergedPos)
        PNL=self.BT.GenPNL(mergedPos,dailyreturn)
        PNLreview=self.Alpha(PNL)
        return(mergedPos,PNL,PNLreview)
    
    def DailyModel(self,dailyreturn,rebalday):
        today=date.today()
        todayname=str(today.strftime("%Y-%m-%d"))
        MarkettopPos=self.PartA_Shen7TopDaily(dailyreturn,rebalday)
        ConsensusPos=self.PartC_Consensus(dailyreturn,rebalday)
        Shen56Pos=self.PartD_Shen56CSIDaily(dailyreturn, rebalday)
        #RPPos=self.PartE_RP(dailyreturn,rebalday)
        #BySector=self.PartB_BySector(dailyreturn, rebalday)
        MarkettopPos,ConsensusPos,Shen56Pos=map(lambda df: df.sort_values(by=['PortNav%'],ascending=False),[MarkettopPos,ConsensusPos,Shen56Pos])
        MarkettopPos.to_csv("D:/CompanyData/Markettop40Pos_"+todayname+".csv",index=False)
        ConsensusPos.to_csv("D:/CompanyData/ConsensusPos_"+todayname+".csv",index=False)
        Shen56Pos.to_csv("D:/CompanyData/Shen56Pos_"+todayname+".csv",index=False)
        #BySector.to_csv("D:/CompanyData/BySector_"+todayname+".csv",index=False)
        #RPPos.to_csv("D:/CompanyData/ROE_PE_"+todayname+".csv",index=False)
        MarkettopPos['PortNav%']=MarkettopPos['PortNav%']*0.3
        ConsensusPos['PortNav%']=ConsensusPos['PortNav%']*0.3
        Shen56Pos['PortNav%']=Shen56Pos['PortNav%']*0.4
        #RPPos['PortNav%']=RPPos['PortNav%']*0.15
        #BySector['PortNav%']=BySector['PortNav%']*0.15
        appended_port=MarkettopPos.append([ConsensusPos,Shen56Pos])
        mergedPos=self.BST.Merge_port(appended_port)
        mergedPosNew=self.SimpMerged(dailyreturn,mergedPos)
        mergedPosNew=self.Risk.RiskMgmt(mergedPosNew)
        mergedPosNew=mergedPosNew.sort_values(by=['PortNav%'],ascending=[False])
        mergedPosNew['ticker']=[str(x).zfill(6)+' CH' for x in mergedPosNew['ticker']]
        mergedPosNew=mergedPosNew.rename(columns={'PortNav%':'weight'})
        mergedPosNew=mergedPosNew[['ticker','weight']]
        mergedPosNew=mergedPosNew.sort_values(by=['weight'],ascending=False)
        newtodayname=todayname.replace("-",'')
        mergedPosNew.to_csv("D:/CompanyData/ModelPortfolio_APort_"+newtodayname+".csv",index=False)
        return(mergedPosNew)
        