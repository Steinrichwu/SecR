# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 16:00:02 2020

@author: wudi
"""
import pandas as pd
import numpy as np
from FundaStock import Funda as FundaStockFund
from FundaStock import Prep as FundaStockPrep
from FundaStock import SuperTrend as FundaSuperTrend
from FundaStock import Review as FundaReview
from Toolbox import DataCollect 
from Toolbox import WeightScheme
from Toolbox import DataStructuring
from Toolbox import ReturnCal
from HotStock import Review as HSReview

DC=DataCollect()
DS=DataStructuring()
WS=WeightScheme()
ST=FundaSuperTrend()
R=FundaReview()
F=FundaStockFund()
FP=FundaStockPrep()
RC=ReturnCal()
HSR=HSReview()
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

class DailyProduction():
    def __init__(self):
        self.P=Prep()
        self.PC=PortConst()
    
    def A_B_CMerge_Daily(self,A,B,C):
        nonsecpos=A.copy()
        simpallsecpos=B.copy()
        hotstock=C.copy()
        nonsecpos['PortNav%']=0.3*nonsecpos['PortNav%']
        simpallsecpos['PortNav%']=0.4*simpallsecpos['PortNav%']
        hotstock['PortNav%']=0.3*hotstock['PortNav%']
        nonsecpos,simpallsecpos,hotstock=map(lambda df: df[['date','ticker','PortNav%']],[nonsecpos,simpallsecpos,hotstock])
        port=pd.DataFrame(columns=['date','ticker','PortNav%'])
        port=port.append([nonsecpos,simpallsecpos,hotstock])
        port['index']=port['date']+port['ticker']
        port['PortNav%']=port.groupby('index')['PortNav%'].transform('sum')
        port=port.drop_duplicates()
        return(port)
    
    def CompositePort_Daily(self,dailyreturn,rebaldaylist):
        Aa,Ab,Ba,Bb,Bc=self.PC.MethodTest(dailyreturn,rebaldaylist)
        activepickhist=HSR.ActivepickNS_production(dailyreturn,rebaldaylist,60)
        pickhist=HSR.BM_intersect_allsec2(activepickhist,dailyreturn)
        port=self.A_B_CMerge_Daily(Ab,Ba,pickhist)
        return(port,Ab,Ba,pickhist)


class Prep():
    def __init__(self):
        pass
    
    
    #Cut the members of each day's allsec portfolio from 400ish to 60 per sector (representing 70% of position usually),and inflate the total stock weights back to 100%
    def Simple_secweight(self,allsecpos,dailyreturn,rebaldaylist):
        allsecpos=WS.SecWeight(allsecpos)
        allsecpos['PortNav%']=allsecpos['PortNav%']*allsecpos['secweight']
        allsecposTop60=allsecpos.groupby('date').apply(lambda x: x.nlargest(60,'PortNav%')).reset_index(drop=True)
        allsecposTop60['totalweight']=allsecposTop60.groupby('date')['PortNav%'].transform('sum')
        allsecposTop60['PortNav%']=allsecposTop60['PortNav%']/allsecposTop60['totalweight']
        return(allsecposTop60)
    

#Choose the topx% of x stocks of each sector/or the market, based or not based on Shen56
class SectorSelectTop():
    def __init__(self):
        pass
    
    #Select top10 stocks of each sector, combine it with (union) of each sectors' in Shen56, give out each sector's  marketcap weight
    #Return the position of stocks,based or not based on Shen56， universetype='Shen56' or 'N.Give out %PortNav
    def SectorTop_holding(self,dailyreturn,rebaldaylist,ranktype,universetype,market_sector):
        universe=R.StockScreen(dailyreturn,rebaldaylist,ranktype,universetype,market_sector)
        universe=universe.drop_duplicates()
        if market_sector=='sector':
            universe=WS.Generate_PortNavMcap_allsecs(universe,dailyreturn)
            STAR=universe.loc[universe['primecode'].isnull()==True,:]
        else:
            universe['stock_count']=universe.groupby('date')['ticker'].transform('count')
            universe['PortNav%']=1/universe['stock_count']
            STAR=''
        return(STAR,universe)                    #因为没有数据，科创板需要单独拿出来

    #PNL of the top10stocks of each stocks+Shen56 universe,  Weihted by Mcap, return cumulative return
    def SectorTop_BT(self,dailyreturn,rebaldaylist,ranktype):
        STAR,universe_weight=self.SectorTop_holding(dailyreturn,rebaldaylist,ranktype)
        universe_weight=universe_weight.loc[universe_weight['primecode'].isnull()==False,:].copy()
        primecodelist=list(universe_weight['primecode'].unique())
        pnltab=pd.DataFrame()
        for primecode in primecodelist:
            secpnl=RC.DailyPNL(dailyreturn,universe_weight.loc[universe_weight['primecode']==primecode,:])
            secpnl[primecode]=np.exp(np.log1p(secpnl['dailyreturn']).cumsum())
            if pnltab.shape[0]==0:
                pnltab=secpnl[['date',primecode]].copy()
            else:
                pnltab=pd.merge(pnltab,secpnl[['date',primecode]],on='date',how='left')
        return(pnltab)
    

class PortConst():
    def __init__(self):
        self.P=Prep()
        self.SST=SectorSelectTop()
    
    #Generate the universal Top40 of shen56based,or all-market based. 
    def PartA(self,dailyreturn,rebaldaylist):
        STAR,Shen56Based_Top40pos=self.SST.SectorTop_holding(dailyreturn,rebaldaylist,'Top40','Shen56','market')
        STAR,marketBased_Top40pos=self.SST.SectorTop_holding(dailyreturn,rebaldaylist,'Top40','N','market')
        return(Shen56Based_Top40pos,marketBased_Top40pos)
    
    #Bactktest the eq weight return of the top40 universal stocks generated by PartA
    #Top40pos1,Top40pos12=PartA(dailyreturn,rebaldaylist)
    def PartA_BT(self,dailyreturn,rebaldaylist,Top40pos):
        Top40pos['stock_count']=Top40pos.groupby('date')['ticker'].transform('count')
        Top40pos['PortNav%']=1/Top40pos['stock_count']
        Top40PNL=RC.DailyPNL(dailyreturn,Top40pos)
        Top40PNL['CumReturn']=np.exp(np.log1p(Top40PNL['dailyreturn']).cumsum())
        return(Top40PNL)
    
    #Generate the stockpools of Shen56based, all-market based, merge of these two of Top15 of each sector    
    def PartB(self,dailyreturn,rebaldaylist):
        STAR,Shen56Based_SecTop15=self.SST.SectorTop_holding(dailyreturn,rebaldaylist,'Top15','Shen56','sector')
        STAR,marketBased_SecTop15=self.SST.SectorTop_holding(dailyreturn,rebaldaylist,'Top15','N','sector')
        universe=pd.read_csv("D:/SecR/Shen56New.csv")
        universe['ticker']=[str(x).zfill(6)for x in universe['ticker']]
        universe=DS.Rebalday_alignment(universe,rebaldaylist)
        universe=universe.drop_duplicates()
        universe=DC.Sector_get(universe,'CSI')
        marketBased_SecTop15=marketBased_SecTop15[['date','ticker','primecode']]
        sec_Top15merged=universe.append(marketBased_SecTop15)
        sec_Top15merged=sec_Top15merged.drop_duplicates()
        return(Shen56Based_SecTop15,marketBased_SecTop15,sec_Top15merged)
    
    #Allocate weights WITHIN sector of pos generated by PartB, and allocate sector weights to stocks weights.
    def PartB_Simpsecweight(self,dailyreturn,rebaldaylist,selectedstocks):
        selectedstocks=WS.Generate_PortNavMcap_allsecs(selectedstocks,dailyreturn)
        simpsecweight_stocks=self.P.Simple_secweight(selectedstocks,dailyreturn,rebaldaylist)
        return(simpsecweight_stocks)
    
    #Add the A(nonsecpos, top40) and the B(CSIweighted sector portfolios),C(Hotstocks) and merge holdings 
    def A_B_CMerge(self,dailyreturn,A,B,C):
        nonsecpos=A.copy()
        simpallsecpos=B.copy()
        hotstock=C.copy()
        nonsecpos['PortNav%']=0.3*nonsecpos['PortNav%']
        simpallsecpos['PortNav%']=0.4*simpallsecpos['PortNav%']
        hotstock['PortNav%']=0.3*hotstock['PortNav%']
        nonsecpos,simpallsecpos,hotstock=map(lambda df: df[['date','ticker','PortNav%']],[nonsecpos,simpallsecpos,hotstock])
        port=pd.DataFrame(columns=['date','ticker','PortNav%'])
        port=port.append([nonsecpos,simpallsecpos,hotstock])
        port['index']=port['date']+port['ticker']
        port['PortNav%']=port.groupby('index')['PortNav%'].transform('sum')
        port=port.drop_duplicates()
        portPNL=RC.DailyPNL(dailyreturn,port)
        portPNL['cumPNL']=np.exp(np.log1p(portPNL['dailyreturn']).cumsum())
        return(port,portPNL)
    
    def MethodTest(self,dailyreturn,rebaldaylist):
        PartAa,PartAb=self.PartA(dailyreturn,rebaldaylist)
        PartBa,PartBb,PartBc=self.PartB(dailyreturn,rebaldaylist)
        PartBa=self.PartB_Simpsecweight(dailyreturn,rebaldaylist,PartBa)
        PartBb=self.PartB_Simpsecweight(dailyreturn,rebaldaylist,PartBb)
        PartBc=self.PartB_Simpsecweight(dailyreturn,rebaldaylist,PartBc)
        PartAa,PartAb,PartBa,PartBb,PartBc=map(lambda df: df[['date','ticker','PortNav%']],[PartAa,PartAb,PartBa,PartBb,PartBc])
        return(PartAa,PartAb,PartBa,PartBb,PartBc)
    
    
    #THIS IS THE FINAL VERSION OF THREEPART COMBINED BACKTEST
    def CompositePort(self,dailyreturn,rebaldaylist):
        Aa,Ab,Ba,Bb,Bc=self.MethodTest(dailyreturn,rebaldaylist)
        activepickhist=HSR.ActivepickNS_production(dailyreturn,rebaldaylist,70)
        pickhist,pickPNL=HSR.BM_intersect_allsec(activepickhist,dailyreturn)
        #pickhist,pickPNL=HSR.BM_intersect_allsec2(activepickhist,dailyreturn)
        port,portPNL=self.A_B_CMerge(dailyreturn,Ab,Ba,pickhist)
        return(port,portPNL)

    
    def Generate_PerSectorPNL(self,dailyreturn,rebaldaylist):
        STAR,Shen56Based_SecTop15=self.SST.SectorTop_holding(dailyreturn,rebaldaylist,'Top5','Shen56','sector')
        seclist=list(Shen56Based_SecTop15['primecode'].unique())
        seclist = [sec for sec in seclist if str(sec) != 'nan']
        secname=DC.Sec_name('CITIC')
        secnamedict=dict(zip(secname['sector'],secname['sectorname']))
        secreturn=pd.DataFrame(columns=['date','dailyreturn','ticker'])
        for sec in seclist:
            print(sec)
            secport=Shen56Based_SecTop15.loc[Shen56Based_SecTop15['primecode']==sec,:].copy()
            secPNL=RC.DailyPNL(dailyreturn,secport)
            secPNL['cumreturn']=np.exp(np.log1p(secPNL['dailyreturn']).cumsum())
            secname=secnamedict[sec]
            secPNL.to_csv("D:/SecR/BySector/"+secname+"_Return.csv",index=False)
            #secPNL['ticker']=sec
            #secreturn=secreturn.append(secPNL)
        #bms=WS.BM_sectorweight('2005-11-28','CSI300','CSI') #D
        #bms['secweight']=bms['secweight']/100
        #bms=bms.loc[bms['date']>='2009-12-30',:].copy()
        #bms.columns=['date','ticker','PortNav%']
        #secreturn.columns=['date','dailyreturn','ticker']
        #secPNL=RC.DailyPNL(secreturn,bms)
        #secPNL['cumreturn']=np.exp(np.log1p(secPNL['dailyreturn']).cumsum())
            #secPNL['cumreturn']=np.exp(np.log1p(secPNL['dailyreturn']).cumsum())
            #secname=secnamedict[sec]
            #secPNL.to_csv("D:/SecR/BySector/"+secname+"_Return.csv",index=False)
        return()