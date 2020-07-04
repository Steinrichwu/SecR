# -*- coding: utf-8 -*-
"""
Created on Thu May 14 09:09:20 2020

@author: wudi
"""

import pandas as pd
from MSSQL import MSSQL
from Querybase import Query
from Toolbox import DataCollect
from Toolbox import WeightScheme
from Toolbox import ReturnCal
from Toolbox import DataStructuring 
from HotStock import Review as HotStockReview

Q=Query()
WS=WeightScheme()
RC=ReturnCal()
DS=DataStructuring()
DC=DataCollect()
HR=HotStockReview()

#CSI300 historical trading dates and returns
tradingday=pd.read_csv("D:/SecR/Tradingday.csv")
ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="GS_NTP12") 
#ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 

class Prep():
    def __init__(self):
        pass
    
#analysts and the industries they cover
    def Analyst_covered_sector(self):
        sql="select distinct id, covered_industry from IDEA_USER"
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['analyst_id','covered_industry'])
        df=df.loc[df['covered_industry'].isnull()==False,:]
        df['covered_industry']=[x.encode('latin-1').decode('gbk') for x in df['covered_industry']]
        return(df)
        
#Pull a list of industries in the database idea_industry database
    def Industry(self):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        sql="select NAME from SYS_DICT D where D.TYPE='idea_industry'"
        reslist=ms.ExecNonQuery(sql)
        df=pd.DataFrame(reslist.fetchall())
        df.columns=['Industry']
        industry_list=df['Industry'].unique()
        industry_list=[x.strip() for x in industry_list]
        return(industry_list)    

#Download top5 analyst' holding of everyrebalday        
    def Holding_query(self,df):
        datelist=df['date'].unique().tolist()
        sql=[]
        for rebalday in datelist:
            analystlist=df.loc[df['date']==rebalday,'analyst_id'].unique().tolist()
            sqlpart="Select '"+rebalday+"' as date, MH.analyst_id,MH.ticker from MODEL_HOLDING MH where date='"+str(rebalday)+"' and analyst_id in("+str(analystlist)[1:-1]+") and ticker not like '%.HK%'"
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        df_holding=pd.DataFrame(reslist,columns=['date','analyst_id','ticker'])
        return(df_holding)

#Delete HK stocks, and delete the 'CH'
    def clean_up_ticker(self,df):
        df['analyst_id']=[x.strip()for x in df['analyst_id']]
        df2=df.loc[~df['ticker'].str.contains(".HK")==True].copy()
        df2['ticker']=df2['ticker'].astype(str).str[:6]
        return(df2)
        
#Input: historical rebalday and selected analysts, Output: stockholding and %PortVa of every rebalday
    def Get_holding(self,dailyreturn,df,benchmark,rebaldaylist):
        df_holding=self.Holding_query(df)
        df_holding=self.clean_up_ticker(df_holding)
        df_holding=df_holding[['date','ticker']]
        df_holding=df_holding.drop_duplicates()
        df_holding_active=WS.Active_stock_screening(df_holding,dailyreturn,rebaldaylist)
        df_holidng_bm=WS.Benchmark_intersect(df_holding_active,benchmark)
        postab=WS.Generate_PortNav(df_holidng_bm)
        return(df_holding_active,postab)

#Get the analyst_rolling P&L on rebaldays, rebaldays in one go
    def General_prep(self,lookback_period,rebaldaylist):
        sql=[]
        for rebalday in rebaldaylist:
            sqlpart="Select '"+rebalday+"' as date, MP.analyst_id, SUM(MP.dtd_chg) as cumPNL from MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1 and date>=dateadd(day,-"+str(lookback_period)+",'"+rebalday+"') and date<'"+rebalday+"' GROUP by MP.analyst_id"
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        analyst_pnl=pd.DataFrame(reslist,columns=['date','analyst_id','cumPNL'])
        return(analyst_pnl)
        
class Top_analyst():
    def __init__(self):
        self.P=Prep()
    
    #Runtime 6', Generate the holding of top 30% total return analysts in the past 60 days
    def Top_analyst_nonSector(self,dailyreturn,rebaldaylist,lookback_period,benchmark):
        analyst_pnl=self.P.General_prep(lookback_period,rebaldaylist)
        print('getting analyst')
        top30p_analyst=pd.DataFrame(columns=['date','analyst_id','cumPNL'])
        for rebalday in rebaldaylist:
            rebalday_analyst=analyst_pnl.loc[(analyst_pnl['date']==rebalday)&(analyst_pnl['cumPNL']!=0),:]
            analystnum=int(rebalday_analyst.shape[0]*0.1)
            top30p_analyst_rebalday=rebalday_analyst.nlargest(analystnum,'cumPNL',keep='all')
            top30p_analyst=top30p_analyst.append(top30p_analyst_rebalday)
        print('downloading holding')
        acitvepicks,TA_holding=self.P.Get_holding(dailyreturn,top30p_analyst,benchmark,rebaldaylist)
        return(acitvepicks,TA_holding,top30p_analyst)
 
    #Runtime 45''Generate the holding of top 5 return analysts of each sector in the past 60 days
    def Top_analyst_Sector(self,dailyreturn,rebaldaylist,lookback_period,benchmark):
        industry_list=self.P.Industry()
        analyst_covered_sector=self.P.Analyst_covered_sector()
        analyst_pnl=self.P.General_prep(lookback_period,rebaldaylist)
        top5_analyst=pd.DataFrame(columns=['date','analyst_id','cumPNL'])
        print('getting analyst')
        for rebalday in rebaldaylist:
            top5_analyst_rebalday=pd.DataFrame(columns=['date','analyst_id','cumPNL'])
            for industry in industry_list:
                analystlist=analyst_covered_sector.loc[(analyst_covered_sector['covered_industry'].str.contains(industry))&(~analyst_covered_sector['covered_industry'].str.contains(','+industry))]['analyst_id'].unique()
                top5_of_industry=analyst_pnl.loc[(analyst_pnl['analyst_id'].isin(analystlist))&(analyst_pnl['date']==rebalday)&(analyst_pnl['cumPNL']!=0),:].nlargest(5,'cumPNL',keep='all')
                top5_analyst_rebalday=top5_analyst_rebalday.append(top5_of_industry)
            top5_analyst=top5_analyst.append(top5_analyst_rebalday)
        print('downloading holding')
        acitvepicks,TA_holding=self.P.Get_holding(dailyreturn,top5_analyst,benchmark,rebaldaylist)
        return(acitvepicks,TA_holding,top5_analyst)

class Niu2():
    def __init__(self):
        self.P=Prep()

#downlaod the rolling 60day return and benchmark returns of analsyts
    def Analyst_history(self):
        sql="select convert(varchar,MP.date, 23) as date, MP.analyst_id, SUM(MP.dtd_chg) OVER (PARTITION by MP.analyst_id Order by date rows between 59 preceding and current row)as CumReturn, SUM(MP.benchmark_dtd_chg) OVER (PARTITION by MP.analyst_id Order by date rows between 59 preceding and current row)as CumBenchmark from MODEL_PORTFOLIO MP where  IsNumeric(analyst_id) = 1 ORDER BY analyst_id "
        reslist=ms.ExecQuery(sql)
        df=pd.DataFrame(reslist,columns=['date','analyst_id','CumReturn','CumBenchmark'])
        df=df.loc[df['date'].isin(tradingday['date']),:]
        df.to_csv("D:/SecR/analyst_GS_TP12.csv",index=False)
        return(df)    
        
#Return the rank of all analysts' historical qmean of 60day rollingAlpha,The KeyFunction of Niu2
    def Analyst_rank_total(self):
        analyst=pd.read_csv("D:/SecR/analyst_GS_TP12.csv")            #all analyst' 60day cumulativer return and 60day cumulative Benchmarket return              
        analyst['RollingAlpha']=analyst['CumReturn']-analyst['CumBenchmark']
        analyst_pivot=analyst.pivot_table(index=['date'],columns='analyst_id',values='RollingAlpha') #pivot, analysts as column names
        analyst_tier=DS.Dfquantile(analyst_pivot) #Dfquantile and Dfpivot are 1-1 direct translation of Rolling Alpha                            #Everyday's RollingAlpha into 5 group
        analyst_qmean=DS.Dfmean(analyst_tier)     #All wrangling issues happen here in Qmean               #Calculate the meann of quintiles since the beginning
        analyst_rank=DS.Dfrank(analyst_qmean)
        analyst_rank['date']=analyst_pivot.index
        analyst_rank.columns=[str(x)for x in analyst_rank.columns]
        return(analyst_rank)
    
     #Runtime 57'' The core of Niu2
    def Top_analyst_Niu2(self,dailyreturn,rebaldaylist,benchmark):
        analyst_covered_sector=self.P.Analyst_covered_sector()
        industry_list=self.P.Industry()
        top5_analyst=pd.DataFrame(columns=['date','analyst_id'])
        analyst_rank=self.Analyst_rank_total()                   #This is the core of Niu2 data analysis
        print('prep done')
        for rebalday in rebaldaylist:
            print(rebalday)
            top5_analyst_rebalday=pd.DataFrame(columns=['date','analyst_id'])
            for industry in industry_list:
                sec_analystlist=analyst_covered_sector.loc[(analyst_covered_sector['covered_industry'].str.contains(industry))&(~analyst_covered_sector['covered_industry'].str.contains(','+industry))]['analyst_id'].unique().tolist()
                sector_analyst_rank=analyst_rank.loc[analyst_rank['date']==rebalday,sec_analystlist]
                sector_analyst_top=sector_analyst_rank.apply(lambda s: s.nlargest(5).index,axis=1).tolist()[0].tolist()
                top5_analyst_rebalday_indu=pd.DataFrame(sector_analyst_top,columns=['analyst_id'])
                top5_analyst_rebalday_indu['date']=rebalday
                top5_analyst_rebalday_indu=top5_analyst_rebalday_indu[['date','analyst_id']]
                top5_analyst_rebalday=top5_analyst_rebalday.append(top5_analyst_rebalday_indu)
            top5_analyst=top5_analyst.append(top5_analyst_rebalday)
        print('getting holding')
        acitvepicks,TA_holding=self.P.Get_holding(dailyreturn,top5_analyst,benchmark,rebaldaylist)
        return(acitvepicks,TA_holding,top5_analyst)
        
class Review():
    def __init__(self):
        self.TA=Top_analyst()
        self.N=Niu2()
        
    def Niu2SectorPNLvsBM(self,dailyreturn,startdate,rebal_period,benchmark,primecodelist):
        postabStrat=self.N.Top_analyst_Niu2(dailyreturn,startdate,rebal_period,benchmark)
        postabStrat=postabStrat.rename(columns={'PortNav%':'weight'})
        postabSec=RC.Comp_sec_postab(postabStrat,primecodelist)
        comptab=RC.SectorPNLvsBM(dailyreturn,postabSec,benchmark,primecodelist)
        return(comptab)
                   
    #Generate the P&L of a windsector, and the sectors P&L in its benchmark index
    def WindsectorPNL(self,startdate,rebal_period,benchmark,windsector):
        sector_summary=pd.read_csv("U:/S/SecR/Sector_summary.csv")
        primseclist=list(sector_summary.loc[sector_summary['NewWind']==windsector,'PrimSecCode'].astype(str))
        comptab=self.Niu2SectorPNLvsBM(startdate,rebal_period,benchmark,primseclist)
        return(comptab)
    
    #dailyreturn=pd.read_csv("U:/SecR/DailyReturn.csv")
    #dailyreturn['ticker']=[x[0:6]for x in dailyreturn['ticker']]
    #startdate='2015-12-28'
    #rebal_period=10
    #lookback_period=30
    #benchmark='CSI300'
    def PNLCal(self,dailyreturn,startdate,rebal_period,lookback_period,benchmark):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        postab=self.N.Top_analyst_Niu2(dailyreturn,rebaldaylist,benchmark)
        #postab=self.TA.Top_analyst_Sector(dailyreturn,rebaldaylist,startdate,rebal_period,lookback_period,benchmark)
        #postab=self.TA.Top_analyst_nonSector(dailyreturn,rebaldaylist,startdate,rebal_period,lookback_period,benchmark)
        SPNL=RC.DailyPNL(dailyreturn,postab)
        CumPNL=RC.CumPNL(SPNL)
        return(CumPNL)
    
    def TApostab(self,dailyreturn,startdate,rebal_period,lookback_period,benchmark):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        activepicksNiu2,postabNiu2,topanalystNiu2=self.N.Top_analyst_Niu2(dailyreturn,rebaldaylist,benchmark)
        print("Niu2Done")
        activepicksSec,postabSec,topanalystSec=self.TA.Top_analyst_Sector(dailyreturn,rebaldaylist,lookback_period,benchmark)
        print("SecDone")
        activepicksNS,postabNS,topanalystNS=self.TA.Top_analyst_nonSector(dailyreturn,rebaldaylist,lookback_period,benchmark)
        print("NonSecDone")
        activepicksHS=HR.ActivepickBMSec_production(dailyreturn,'N',rebaldaylist)
        return(activepicksNiu2,activepicksSec,activepicksNS,activepicksHS)
    
    #For the GS_TP12 dtabase, startdate should be '2006-12-28' not '2005-12-28'
    def TAfour(self,dailyreturn,startdate,rebal_period,lookback_period,benchmark):
        N2,Sec,NS,HS=self.TApostab(dailyreturn,startdate,rebal_period,lookback_period,benchmark)
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
        TAfourPNL=RC.EqReturn(dailyreturn,TAcount)
        return(TAfourPNL)