# -*- coding: utf-8 -*-
"""
Created on Fri May 15 22:18:22 2020

@author: wudi
"""
import pandas as pd
import numpy as np
from MSSQL import MSSQL
from Toolbox import DataCollect
from Toolbox import WeightScheme
from Toolbox import DataStructuring
from Toolbox import ReturnCal

WS=WeightScheme()
DC=DataCollect()
DS=DataStructuring()
RC=ReturnCal()
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD    
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

class QueryMgmt():
    def __init__(self):
        pass
    
        #PastNdays recommended stocks with no tickerlist
    def Hotstock_query(self,rebalday,lookback_period):
        sqlpart="Select '"+rebalday+"' as date, code, count(*) Reccount from jyzb_new_1.dbo.cmb_report_research R left join jyzb_new_1.dbo.I_SYS_CLASS C on C.SYS_CLASS=R.score_id left join jyzb_new_1.dbo.I_ORGAN_SCORE S on S.ID=R.organ_score_id where into_date>=dateadd(day,-"+str(lookback_period)+",'"+rebalday+"') and into_date<'"+rebalday+"' and (sys_class=7 OR sys_class=5) GROUP BY code ORDER BY Reccount DESC "
        return(sqlpart)
    
        
        
class Prep():
    def __init__(self):
        self.Q=QueryMgmt()
    
    #Construct the query of hotstock across different rebalday
    def Hotstock_nonsectorQuery(self,rebaldaylist,lookback_period):
        sql=[]
        for rebalday in rebaldaylist:
            sqlpart=self.Q.Hotstock_query(rebalday,lookback_period)
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','raccount'])
        return(rechist)
    
    
    #Construct the query of hotstocks of a sector across different rebalday
    def Hotsotck_sectorQuery(self,rebaldaylist,lookback_period,df):
        sql=[]
        for rebalday in rebaldaylist:
            sqlpart=self.Q.Hotstock_query(rebalday,lookback_period)
            sql.append(sqlpart)
        reslist=ms.ExecListQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','raccount'])
        rechist_sector=pd.DataFrame(columns=['date','ticker','raccount'])
        for rebalday in rebaldaylist:
            tickerlist=df.loc[df['date']==rebalday,'ticker'].tolist()
            rechist_rebalday=rechist.loc[(rechist['date']==rebalday)&(rechist['ticker'].isin(tickerlist)),:]
            rechist_sector=rechist_sector.append(rechist_rebalday)
        return(rechist_sector)
    
    #Select top xstocks stocks from a df using colindex
    def Top_stocks(self,df,colindex,topx,allnot):
        rebaldaylist=df['date'].unique()
        dfnew=pd.DataFrame(columns=df.columns)
        df[colindex]=df[colindex].astype(float)
        for rebalday in rebaldaylist:
            if allnot=='all':
                dfrebalday=df.loc[df['date']==rebalday,:].nlargest(topx,colindex,keep='all')
            else:
                dfrebalday=df.loc[df['date']==rebalday,:].nlargest(topx,colindex)
            dfnew=dfnew.append(dfrebalday)
        return(dfnew)
        
        
class StockPick():
    def __init__(self):
        self.P=Prep()
     
    #startdate='2015-12-28',benchmark='CSI300', rebal_period=10,lookback_period=30, primecodelist=[40,36],topx=60
    #Choose a benchmark, return top xstocks of most recommended stocks (OF a sector) among benchmark memb stocks
    #p=SP.Rec_stat_benchmark(startdate,benchmark,rebal_period,topx,lookback_period,'N')
    #p=SP.Rec_stat_benchmark(startdate,benchmark,rebal_period,topx,lookback_period,[40,36])
    def Rec_stat(self,dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher):
        if primecodelist=='N':
            rechist=self.P.Hotstock_nonsectorQuery(rebaldaylist,lookback_period)
        else:
            sectorstock=DC.Sector_stock(rebaldaylist,primecodelist,publisher)
            rechist=self.P.Hotsotck_sectorQuery(rebaldaylist,lookback_period,sectorstock)
        #rechist_active=WS.Active_stock_screening(rechist,dailyreturn,rebaldaylist)
        return(rechist)
    
    #postab=SP.Rec_stat_benchmark(dailyreturn,startdate,'CSI300',70,10,30,'N')
    def Rec_stat_benchmark(self,dailyreturn,benchmark,topx,lookback_period,primecodelist,rebaldaylist,publisher):
        rechist_active=self.Rec_stat(dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher)
        rechist_bm=WS.Benchmark_intersect(rechist_active,benchmark)
        rechist_bm=rechist_bm.sort_values(['date','raccount','weight'],ascending=[True,False,False])
        rechist_selected=self.P.Top_stocks(rechist_bm,'raccount',topx,'notall')
        postab=WS.Generate_PortNav(rechist_selected)
        return(postab)
    
    def Rec_stat_nonbenchmark(self,dailyreturn,topx,lookback_period,primecodelist,rebaldaylist,publisher):
        rechist_active=self.Rec_stat(dailyreturn,rebaldaylist,lookback_period,primecodelist,publisher)
        mcaphist=DC.Mcap_hist(rebaldaylist,rechist_active)
        rechist_mcap=DS.Data_merge(rechist_active,mcaphist,'mcap')
        rechist_mcap=rechist_mcap.sort_values(['date','raccount','mcap'],ascending=[True,False,False])
        rechist_selected=self.P.Top_stocks(rechist_mcap,'raccount',topx,'notall')
        rechist_selected=rechist_selected.rename(columns={'mcap':'weight'})
        postab=WS.Generate_PortNav(rechist_selected)
        return(postab)

class SecR():
    def __init__(self):
        self.P=Prep()
        self.SP=StockPick()
    
    #Return the rank and recommendation times of each sector on every rebal day
    def SecStats(self,dailyreturn,rebaldaylist,lookback_period,publisher):
        #rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        rechist=self.P.Hotstock_nonsectorQuery(rebaldaylist,lookback_period)  #number of reports of ecah stocks
        rechist=DS.Screen_Ashs(rechist)
        rechist_active=WS.Active_stock_screening(rechist,dailyreturn,rebaldaylist)
        tickerlist=rechist_active['ticker'].unique()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,publisher)
        #totalsecnum=len(stock_sector['primecode'].unique())
        #selectsecsnum=int(totalsecnum*0.3)                    #pikc the top30% or show all recomendations of all sectors
        rechist_active,stock_sector=map(DS.Addindex,(rechist_active,stock_sector))
        rechist_active_sector=pd.merge(rechist_active,stock_sector[['index','primecode']],on='index',how='left')
        rechist_active_sector['primindex']=rechist_active_sector['date']+rechist_active_sector['primecode']
        secrac=pd.DataFrame(rechist_active_sector.groupby(['primindex'])['raccount'].sum())
        secrac.reset_index(inplace=True)
        secrac['date']=secrac['primindex'].str[0:10]
        secrac['sector']=secrac['primindex'].str[10:13]
        for rebalday in rebaldaylist:
            secrac.loc[secrac['date']==rebalday,'rank'] = secrac.loc[secrac['date']==rebalday,'raccount'].rank(ascending=False,method='min')
        #topsec=secrac.loc[secrac['rank']<=selectsecsnum]
        return(secrac)
    
    #Merge the Bank/Nonbank of CITIC into a CSI table, get sector name
    #CSI=SR.Getsecname(dailyreturn,rebaldaylist,lookback_period)
    #CSI.to_csv("D:/SecR/Hotsector.csv",encoding='utf-8-sig',index=False)
    def Getsecname(self,dailyreturn,rebaldaylist,lookback_period):
        CSI=self.SecStats(dailyreturn,rebaldaylist,lookback_period,'CSI')
        CITIC=self.SecStats(dailyreturn,rebaldaylist,lookback_period,'CITIC')
        sec_nameCSI=DC.Sec_name('CSI')
        sec_nameCITIC=DC.Sec_name('CITIC')
        #sec_name['sectorname']=[x.encode('latin-1').decode('gbk') for x in sec_name['sectorname']]
        sec_nameCSI,sec_nameCITIC=map(lambda df: df.drop_duplicates(['sector'],keep="first"),[sec_nameCSI,sec_nameCITIC])
        CSI=pd.merge(CSI,sec_nameCSI,how='left',on='sector')
        CITIC=pd.merge(CITIC,sec_nameCITIC,how='left',on='sector')
        CSI,CITIC=map(lambda df:df[['date','sector','sectorname','raccount']],[CSI,CITIC])
        CSIstockcount=self.Secstockcount(CSI,'CSI',dailyreturn)
        CITICstockcount=self.Secstockcount(CITIC,'CITIC',dailyreturn)
        CSI['index']=CSI['date']+CSI['sector']
        CSIstockcount['index']=CSIstockcount['date']+CSIstockcount['sector']
        CSI=pd.merge(CSI,CSIstockcount[['index','stockcounts']],on='index',how='left')
        CITIC['index']=CITIC['date']+CITIC['sector']
        CITICstockcount['index']=CITICstockcount['date']+CITICstockcount['sector']
        CITIC=pd.merge(CITIC,CITICstockcount[['index','stockcounts']],on='index',how='left')
        CSI=CSI.loc[CSI['sector']!='06',:]
        CSI=CSI.append(CITIC.loc[CITIC['sector'].isin(['40','41','42']),:])
        CSI['coverage']=CSI['raccount']/CSI['stockcounts']
        CITIC['coverage']=CITIC['raccount']/CITIC['stockcounts']
        #topsec=topsec.sort_values(by=['rank'])
        CSI=CSI.sort_values(by=['date','coverage'],ascending=[True,False])
        CITIC=CITIC.sort_values(by=['date','coverage'],ascending=[True,False])
        #CSI['sectorname']=[x.encode('latin-1').decode('gbk') for x in CSI['sectorname']]
        #CITIC['sectorname']=[x.encode('latin-1').decode('gbk') for x in CITIC['sectorname']]
        CSI=CSI[['date','sector','sectorname','raccount','stockcounts','coverage']]
        CITIC=CITIC[['date','sector','sectorname','raccount','stockcounts','coverage']]
        return(CSI,CITIC)
    
    #Stats on how many stocks in total in each sector on every rebalday 
    def Secstockcount(self,df,publisher,dailyreturn):
        primecodelist=list(df['sector'].unique())
        rebaldaylist=list(df['date'].unique())
        sector_stock=DC.Ashs_stock_seccode(rebaldaylist,primecodelist,publisher)
        sector_stock=DC.Mcap_get(sector_stock,dailyreturn)
        lcpd=DC.Lowmarketcap_rebaldaylist(sector_stock,dailyreturn)
        sector_stock=pd.merge(sector_stock,lcpd,on='date',how='left')
        sector_stock=sector_stock.loc[sector_stock['mcap']>sector_stock['lowmcapbar'],:]      #Only stocks>30%mcap are retained
        sector_stock['newindex']=sector_stock['date']+sector_stock['primecode']
        stockcount=pd.DataFrame(sector_stock['newindex'].value_counts())
        stockcount.reset_index(inplace=True)
        stockcount['date']=stockcount['index'].str[0:10]
        stockcount['sector']=stockcount['index'].str[10:]
        stockcount=stockcount.rename(columns={'newindex':'stockcounts'})
        stockcount=stockcount[['date','sector','stockcounts']]
        return(stockcount)
    
    #Measure the ratio of published reports vs number of top70%mcap stocks in a sector 
    #Return the mix_rank of absolute raccounts and coverageratio.
    def Coverage(self,dailyreturn,rebaldaylist,lookback_period,publisher):
        secrac=self.SecStats(dailyreturn,rebaldaylist,lookback_period,publisher)
        stockcount=self.Secstockcount(secrac,publisher,dailyreturn)
        stockcount['primindex']=stockcount['date']+stockcount['sector']
        secrac=pd.merge(secrac,stockcount[['primindex','stockcounts']],on='primindex',how='left')
        secrac['coverage_ratio']=secrac['raccount']/secrac['stockcounts']
        for rebalday in rebaldaylist:
            secrac.loc[secrac['date']==rebalday,'coverage_rank'] = secrac.loc[secrac['date']==rebalday,'coverage_ratio'].rank(ascending=False,method='min')
        return(secrac)
    
    #Choose the top25% coverage_rank sectors, and buy stocks of top70%marketcap 
    #Only works for CITIC 分类法
    #参数：startdate='2009-12-28',rebal_period=20,lookback_period=60, publisher='CITIC'
    def SecR_mix_BT(self,dailyreturn,rebaldaylist,lookback_period,publisher):
        secrac=self.Coverage(dailyreturn,rebaldaylist,lookback_period,publisher)
        daylist=list(secrac['date'].unique())
        stocklist_hist=pd.DataFrame(columns=['date','ticker','primecode'])
        for day in daylist:
            secrac_day=secrac.loc[secrac['date']==day,:].copy()
            sectornum=secrac_day.shape[0]
            #secrac_day=secrac_day.nsmallest(int(sectornum*0.3),'rank')
            secrac_day=secrac_day.nsmallest(int(sectornum*0.25),'coverage_rank')
            stocklist=DC.Ashs_stock_seccode([day],list(secrac_day['sector'].unique()),publisher)
            stocklist_hist=stocklist_hist.append(stocklist)
        stocklist_hist=DC.Mcap_get(stocklist_hist,dailyreturn)
        lcpd=DC.Lowmarketcap_rebaldaylist(stocklist_hist,dailyreturn)
        stocklist_hist=pd.merge(stocklist_hist,lcpd,on='date',how='left')
        stocklist_hist=stocklist_hist.loc[stocklist_hist['mcap']>stocklist_hist['lowmcapbar'],:]  
        PNL=RC.EqReturn(dailyreturn,stocklist_hist)
        return(stocklist_hist,PNL)
        
    #Convert the hotsector raccount into each sector's stocks' signals
    def HotsectorSignal(self,dailyreturn,rebaldaylist):
        #Get every stocks' sector return
        secrac=self.SecStats(dailyreturn,rebaldaylist,60)
        primecodelist=list(secrac['sector'].unique())
        stock_sector=DC.Ashs_stock_seccode(rebaldaylist,primecodelist,'CITIC')
        stock_sector['primindex']=stock_sector['date']+stock_sector['primecode']
        stock_sector=pd.merge(stock_sector,secrac[['primindex','raccount']],on='primindex',how='left')
        stock_sector=stock_sector.dropna()
        stock_sector=stock_sector.drop('primindex',1)
        return(stock_sector)
    
    #Top 30% stocks that are most recommended in the past 180days, those not are not in the list arent' even worth a look 
    def Watch_list(self,rebaldaylist,dailyreturn):
        watchlist=self.P.Hotstock_nonsectorQuery(rebaldaylist,180)
        watchlist=DS.Screen_Ashs(watchlist)
        tickerlist=list(watchlist['ticker'].unique())
        stocks_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CITIC')
        watchlist,stocks_sector=map(DS.Addindex,(watchlist,stocks_sector))
        watchlist=pd.merge(watchlist,stocks_sector[['index','primecode']],how='left',on='index')
        watchlist=DC.Mcap_get(watchlist,dailyreturn)
        selected_watchlist=watchlist.nlargest(800,'raccount',keep='all')
        secname=DC.Sec_name('CITIC')
        secname.columns=['primecode','sector']
        selected_watchlist=pd.merge(selected_watchlist,secname,on='primecode',how='left')
        selected_watchlist=selected_watchlist[['date','ticker','sector','raccount','mcap']]
        selected_watchlist['ticker']=[str(x)+" CH" for x in selected_watchlist['ticker']]
        selected_watchlist.to_csv("D:/S/Watch_list.csv",encoding='utf-8-sig',index=False)
        return()
        
class Review():
    def __init__(self):
        self.P=Prep()
        self.SP=StockPick()
    
    #To produce the Hotstock postab on weekly basis according to Wind sector categorization, produce the whole holding history since 2005-12-28 every week
    def Postab_ProductionSec(self,dailyreturn,rebaldaylist):
        sector_summary=pd.read_csv("D:/SecR/Sector_summary.csv")
        sector2=sector_summary[['NewWind','HighReturn']].drop_duplicates(inplace=False) #Mapping of Windsector with Prime Industry of Citics
        postabhist=pd.DataFrame(columns=['date','ticker','PortNav%'])
        for windsector in sector2['NewWind']:
            print(windsector)
            topx=int(sector2.loc[sector2['NewWind']==windsector,'HighReturn'])
            primseclist=list(sector_summary.loc[sector_summary['NewWind']==windsector,'PrimSecCode'].astype(str))
            postab=self.SP.Rec_stat_benchmark(dailyreturn,'CSI800',topx,10,primseclist,rebaldaylist,'CITIC')
            postabhist=postabhist.append(postab)
            postab.to_csv("D:/Hotstocks/"+windsector+".csv",index=False)
        return(postabhist)
        
    #To produce the Hotstock postab on daily basis, with benchmark 
    def ActivepickBM_production(self,dailyreturn,bm,topx,rebaldaylist):
        postab=self.SP.Rec_stat_benchmark(dailyreturn,bm,topx,30,'N',rebaldaylist,'CITIC')
        postab.to_csv("D:/Hotstocks/Hotstock_"+bm+".csv",index=False)
        return(postab)
    
    #Pick the top30% of EVERY CSI sector, intersection with a BM.
    def ActivepickBMSec_production(self,dailyreturn,bm,rebaldaylist):
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CSI')
        #rechist_bm=WS.Benchmark_intersect(rechist_active,bm)       #No intersection with BM
        rechist_bm=rechist_active.copy()
        mcaphist=dailyreturn.loc[(dailyreturn['date'].isin(rechist_bm['date'].unique()))&(dailyreturn['ticker'].isin(rechist_bm['ticker'].unique())),['date','ticker','mcap']]
        rechist_mcap=DS.Data_merge(rechist_bm,mcaphist,'mcap')
        tickerlist=rechist_mcap['ticker'].unique().tolist()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CSI')
        rechist_mcap,stock_sector=map(DS.Addindex,(rechist_mcap,stock_sector))
        rechist_mcap=pd.merge(rechist_mcap,stock_sector[['index','primecode']],on='index',how='left')
        rechist_mcap['index']=rechist_mcap['date']+rechist_mcap['primecode']
        rechist_mcap=rechist_mcap.sort_values(by=['date','raccount','mcap'],ascending=[True,False,False])
        rechist_mcap['nthoccur']=rechist_mcap.groupby('index').cumcount()+1
        indexcounts=pd.DataFrame(rechist_mcap['index'].value_counts())
        indexcounts.reset_index(inplace=True)
        indexcounts.columns=['index','totaloccur']
        rechist_mcap=pd.merge(rechist_mcap,indexcounts,on='index',how='left')
        rechist_mcap['diff']=rechist_mcap['nthoccur']-rechist_mcap['totaloccur']*0.3
        rechist_mcap=rechist_mcap.loc[rechist_mcap['diff']<=0,:]
        rechist_mcap=rechist_mcap[['date','ticker','raccount']]
        rechist_mcap=rechist_mcap.drop_duplicates()
        rechist_mcap=rechist_mcap.reset_index(drop=True)
        return(rechist_mcap)
    
    
    #Pick the top30% of EVERY CSI Mixed sector, NO intersection with a BM.
    def ActivepickBMSec(self,dailyreturn,bm,rebaldaylist):
        rechist_bm=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CSI')
        mcaphist=dailyreturn.loc[(dailyreturn['date'].isin(rechist_bm['date'].unique()))&(dailyreturn['ticker'].isin(rechist_bm['ticker'].unique())),['date','ticker','mcap']]
        rechist_mcap=DS.Data_merge(rechist_bm,mcaphist,'mcap')
        #tickerlist=rechist_mcap['ticker'].unique().tolist()
        #stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CSI')
        stock_sector=WS.Stock_sectorMixed(rechist_bm)
        stock_sector=stock_sector.loc[stock_sector['primecode'].isnull()==False,:].copy()
        stock_sector.loc[stock_sector['primecode'].isin(['4110','4120','4130','42']),'primecode']='41'
        rechist_mcap,stock_sector=map(DS.Addindex,(rechist_mcap,stock_sector))
        rechist_mcap=pd.merge(rechist_mcap,stock_sector[['index','primecode']],on='index',how='left')
        rechist_mcap['index']=rechist_mcap['date']+rechist_mcap['primecode']
        rechist_mcap=rechist_mcap.sort_values(by=['date','raccount','mcap'],ascending=[True,False,False])
        rechist_mcap['nthoccur']=rechist_mcap.groupby('index').cumcount()+1
        indexcounts=pd.DataFrame(rechist_mcap['index'].value_counts())
        indexcounts.reset_index(inplace=True)
        indexcounts.columns=['index','totaloccur']
        rechist_mcap=pd.merge(rechist_mcap,indexcounts,on='index',how='left')
        rechist_mcap.loc[rechist_mcap['totaloccur']<35,'totaloccur']=35
        rechist_mcap['diff']=rechist_mcap['nthoccur']-rechist_mcap['totaloccur']*0.3 #if a sector's all stocks picked is ower than 40, we would make total=40,hence at least 12 stocks selected
        rechist_mcap=rechist_mcap.loc[rechist_mcap['diff']<=0,:]
        rechist_mcap=rechist_mcap[['date','ticker','raccount','primecode']]
        rechist_mcap=rechist_mcap.rename(columns={'primecode':'sector'})
        sec_nameCSI=DC.Sec_name('CSI')
        sec_nameCITIC=DC.Sec_name('CITIC')
        sec_name=sec_nameCSI.append(sec_nameCITIC)
        rechist_mcap=pd.merge(rechist_mcap,sec_name,on='sector',how='left')
        rechist_mcap=rechist_mcap.drop_duplicates()
        rechist_mcap=rechist_mcap.reset_index(drop=True)
        return(rechist_mcap)
    
    
    #Pick the top30% of EVERY CSI sector, NO intersection with a BM.
    def ActivepickSec(self,dailyreturn,rebaldaylist):
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CSI')
        #rechist_bm=rechist_active.copy()
        mcaphist=dailyreturn.loc[(dailyreturn['date'].isin(rechist_active['date'].unique()))&(dailyreturn['ticker'].isin(rechist_active['ticker'].unique())),['date','ticker','mcap']]
        rechist_mcap=DS.Data_merge(rechist_active,mcaphist,'mcap')
        tickerlist=rechist_mcap['ticker'].unique().tolist()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist,'CSI')
        rechist_mcap,stock_sector=map(DS.Addindex,(rechist_mcap,stock_sector))
        rechist_mcap=pd.merge(rechist_mcap,stock_sector[['index','primecode']],on='index',how='left')
        rechist_mcap['index']=rechist_mcap['date']+rechist_mcap['primecode']
        rechist_mcap=rechist_mcap.sort_values(by=['date','raccount','mcap'],ascending=[True,False,False])
        rechist_mcap['nthoccur']=rechist_mcap.groupby('index').cumcount()+1
        indexcounts=pd.DataFrame(rechist_mcap['index'].value_counts())
        indexcounts.reset_index(inplace=True)
        indexcounts.columns=['index','totaloccur']
        rechist_mcap=pd.merge(rechist_mcap,indexcounts,on='index',how='left')
        rechist_mcap['diff']=rechist_mcap['nthoccur']-rechist_mcap['totaloccur']*0.3
        rechist_mcap=rechist_mcap.loc[rechist_mcap['diff']<=0,:]
        rechist_mcap=rechist_mcap[['date','ticker','raccount','primecode','mcap']]
        rechist_mcap=rechist_mcap.rename(columns={'primecode':'sector'})
        sec_nameCSI=DC.Sec_name('CSI')
        rechist_mcap=pd.merge(rechist_mcap,sec_nameCSI,on='sector',how='left')
        rechist_mcap=rechist_mcap.drop_duplicates()
        rechist_mcap=rechist_mcap.reset_index(drop=True)
        rechist_mcap.to_csv("D:/CompanyData/Hotstocks_history.csv",encoding='utf-8-sig',index=False)
        return(rechist_mcap)
    
    #Pick the top3 of Banks/Brokers/Insurance. NO intersection with a BM.
    def TopHotFIG(self,dailyreturn,rebaldaylist):
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CSI')
        rechist_active=WS.FIG_Recategorize(rechist_active)
        FIG_active=rechist_active.loc[rechist_active['secondcode'].isin(['40','41','4120','42']),:].copy()
        FIG_active['index']=FIG_active['date']+FIG_active['secondcode']
        FIG_active['count']=FIG_active.groupby(['index'])['index'].transform('count')
        FIG_active['rank']=FIG_active.groupby(['index'])['raccount'].rank("dense", ascending=False)
        FIG_active=FIG_active.loc[FIG_active['rank']<=3,FIG_active.columns.isin(['date','ticker','racount','secondcode'])].copy() #IF tied, then there would be more than 3 stocks shortlisted
        return(FIG_active)
    
    #Produce absolute top topx stocks (with no %PortNav) on each rebalday, no intersection, marketcap or sector 
    def ActivepickNS_production(self,dailyreturn,rebaldaylist,topx):    
        rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,'N','CITIC')
        activepickhist=self.P.Top_stocks(rechist_active,'raccount',topx,'all')
        return(activepickhist)
    
    def BM_intersect_allsec(self,activepickhist,dailyreturn):
        pickhist=WS.Benchmark_intersect(activepickhist,'CSI300')
        pickhist=WS.Generate_PortNavMcap(pickhist,dailyreturn)
        pickPNL=RC.DailyPNL(dailyreturn,pickhist)
        pickPNL['CumPNL']=np.exp(np.log1p(pickPNL['dailyreturn']).cumsum())
        return(pickhist,pickPNL)
    
    #Cap the holding of Moutai at 5% in the hotstock subportfolio, take CSI300 intersection and use Mirror weight
    def BM_intersect_allsec2(self,activepickhist,dailyreturn):
        activepickhist=activepickhist.loc[activepickhist['ticker']!='600519',:].copy()
        pickhist=WS.Benchmark_intersect(activepickhist,'CSI300')
        pickhist['totalweight']=pickhist.groupby('date')['weight'].transform('sum')
        pickhist['PortNav%']=pickhist['weight']/pickhist['totalweight']
        pickhist['PortNav%']=pickhist['PortNav%']*0.95
        moutaiholding=pd.DataFrame(list(pickhist['date'].unique()),columns=['date'])
        moutaiholding['ticker']='600519'
        moutaiholding['PortNav%']=0.05
        pickhist=pickhist.append(moutaiholding)
        pickhist=pickhist.sort_values(by=['date','PortNav%'],ascending=[True,False])
        return(pickhist)
    
    def BM_intersect_allsecPos(self,activepickhist,dailyreturn):
        pickhist=WS.Benchmark_intersect(activepickhist,'CSI300')
        pickhist=WS.Generate_PortNavMcap(pickhist,dailyreturn)
        return(pickhist)
    
    #Produce topx picks of each sector on every rebalday according to sector summary table with NO PortNAV%, no intersection and no marketcap
    def ActivepickSec_production(self,startdate,rebal_period,dailyreturn):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        sector_summary=pd.read_csv("D:/SecR/Sector_summary.csv")
        sector2=sector_summary[['NewWind','HighReturn']].drop_duplicates(inplace=False) #Mapping of Windsector with Prime Industry of Citics
        activepickhist=pd.DataFrame(columns=['date','ticker','raccount','mcap'])
        for windsector in sector2['NewWind']:
            print(windsector)
            topx=int(sector2.loc[sector2['NewWind']==windsector,'HighReturn'])
            primseclist=list(sector_summary.loc[sector_summary['NewWind']==windsector,'PrimSecCode'].astype(str))
            rechist_active=self.SP.Rec_stat(dailyreturn,rebaldaylist,30,primseclist)
            mcaphist=DC.Mcap_hist(rebaldaylist,rechist_active)
            rechist_mcap=DS.Data_merge(rechist_active,mcaphist,'mcap')
            rechist_mcap=rechist_mcap.sort_values(['date','raccount','mcap'],ascending=[True,False,False])
            rechist_selected=self.P.Top_stocks(rechist_mcap,'raccount',topx,'notall')
            activepickhist=activepickhist.append(rechist_selected)
        return(activepickhist)
                    
            
    #Compare the sector strategy return and the return of sector in BM
    #dailyreturn=pd.read_csv("U:/S/SecR/DailyReturn.csv")
    #dailyreturn['ticker']=[x[0:6]for x in dailyreturn['ticker']]
    def SectorPNLvsBM(self,dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist):
        postabStrat=self.SP.Rec_stat_benchmark(dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist)
        comptab=RC.SectorPNLvsBM(dailyreturn,postabStrat,benchmark,primecodelist)
        return(comptab)
    
    def Bankinghotstock(self,dailyreturn,rebaldaylist):
        banks=self.SP.Rec_stat(dailyreturn,rebaldaylist,60,['40'],'CITIC')
        banks['raccount']=banks['raccount'].astype(int)
        hotbanks=banks.groupby('date').apply(lambda x: x.nlargest(6,'raccount')).reset_index(drop=True)
        CSI300hotbanks=WS.Benchmark_intersect(hotbanks,'CSI300')
        CSI300hotbanks['totalweight']=CSI300hotbanks.groupby('date')['weight'].transform('sum')
        CSI300hotbanks['PortNav%']=CSI300hotbanks['weight']/CSI300hotbanks['totalweight']
        #CSI300hotbanks=WS.Generate_PortNavMcap(CSI300hotbanks,dailyreturn)
        banksPNL=RC.DailyPNL(dailyreturn,CSI300hotbanks)
        banksPNL['CumPNL']=np.exp(np.log1p(banksPNL['dailyreturn']).cumsum())
        return(banksPNL)
    
    #Combine the rank of recommendation in HK in the past 60 days + past 20 days,pick top50
    def HongKongHotStock(self,rebaldaylist):
        rechist=self.P.Hotstock_nonsectorQuery(rebaldaylist,60)
        rechist2=self.P.Hotstock_nonsectorQuery(rebaldaylist,20)
        rechist=rechist.loc[rechist['ticker'].str.len()==5,:].copy()
        rechist2=rechist2.loc[rechist2['ticker'].str.len()==5,:].copy()
        rechist['longrank']=rechist['raccount'].rank(ascending=True,method='min')
        rechist2['shortrank']=rechist2['raccount'].rank(ascending=True,method='min')
        rechist=pd.merge(rechist,rechist2[['ticker','shortrank']],on='ticker',how='left')
        rechist['rank']=rechist['longrank']+rechist['shortrank']
        rechist['ticker']=[x[1:5]for x in rechist['ticker']]
        rechist['ticker']=[x+'.HK'for x in rechist['ticker']]
        rechist=rechist.nlargest(50,'rank',keep='all')
        rechist=rechist[['date','ticker']].copy()
        return(rechist)
    
    
    #startdate='2015-12-28'
    #topx=70
    #rebal_period=10
    #lookback_period=30
    #primecodelist='N'
    #benchmark='CSI300'
    def PNLCal(self,dailyreturn,startdate,topx,rebal_period,lookback_period,benchmark,primecodelist):
        postab=self.SP.Rec_stat_benchmark(dailyreturn,startdate,benchmark,topx,rebal_period,lookback_period,primecodelist)
        SPNL=RC.DailyPNL(dailyreturn,postab)
        CumPNL=RC.CumPNL(SPNL)
        return(CumPNL)

class Sectop_stocks():
    def __init__(self):
        self.P=Prep()
        self.SP=StockPick()
        self.SR=SecR()
        
    #Return the dailyreturn of every CITIC sector's top five marketcap portfolio and the top 5 stocks of every sector (mean of three)
    def CITICsectop_return(self,dailyreturn,rebaldaylist):
        mcap=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist)),['date','ticker','mcap']].copy()
        mcap_sec=DC.Sector_get(mcap,'CITIC')
        mcap_sec['index']=mcap_sec['date']+mcap_sec['primecode']
        mcapTop=mcap_sec.groupby('index').apply(lambda x: x.nlargest(5,'mcap')).reset_index(drop=True)
        mcapTop2=mcapTop.pivot_table(index='date',columns='ticker',values='mcap',aggfunc='first')
        mcapTop2.reset_index(inplace=True)
        mcapTop2=mcapTop2.fillna(0)
        firstday=rebaldaylist[0]
        tradingday=pd.DataFrame(list(dailyreturn.loc[dailyreturn['date']>=firstday,'date'].unique()),columns=['date'])
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
    def CITICsec_analyst(self,dailyreturn,rebaldaylist):
        CITICsectop_dailyreturn,mcapTop_memb=self.CITICsectop_return(dailyreturn,rebaldaylist)
        CITICsectop_dailyreturn['index']=CITICsectop_dailyreturn['date']+CITICsectop_dailyreturn['primecode']
        tradingday=pd.DataFrame(list(CITICsectop_dailyreturn['date'].unique()),columns=['date'])
        CSI,CITIC=self.SR.Getsecname(dailyreturn,rebaldaylist,30)
        CITICtop=CITIC.groupby('date').apply(lambda x: x.nlargest(6,'coverage')).reset_index(drop=True)
        CITICtop=CITICtop.pivot_table(index='date',columns='sector',values='coverage',aggfunc='first')
        CITICtop=CITICtop.fillna(0)
        CITICtop=pd.merge(tradingday,CITICtop,on='date',how='left')
        CITICtop=CITICtop.fillna(method='ffill')
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

  