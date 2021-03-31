# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 21:33:17 2021

@author: wudi
"""
#Fund Analysis
from MSSQL import MSSQL
from Toolbox import DataCollect
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import datetime
from dateutil.relativedelta import relativedelta
from datetime import date
import empyrical as em
from HotStock import Review as HSReview
from HotStock import Prep as HSPrep
from FundaStock import Funda as FundaStockFund 
from AnalystStock import Review as AReview
from Toolbox import DataStructuring 
from Toolbox import WeightScheme
from Toolbox import ReturnCal

DC=DataCollect()
HP=HSPrep()
HSR=HSReview()
FF=FundaStockFund()
AR=AReview()
DS=DataStructuring()
WS=WeightScheme()
RC=ReturnCal()

#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

class FundDataCollect():
    def __init__(self):
        pass

    #lastquarterend date, used to extract portfolio holding data
    def lastquarterend(self,ref):
        #ref=date.today()
        ref=pd.to_datetime(ref)
        ref=ref-relativedelta(months=1)
        if ref.month < 4:
            lastquarterend=datetime.date(ref.year - 1, 12, 31)
        elif ref.month < 7:
            lastquarterend=datetime.date(ref.year, 3, 31)
        elif ref.month < 10:
            lastquarterend=datetime.date(ref.year, 6, 30)
        else:
            lastquarterend=datetime.date(ref.year, 9, 30)
        lastquarterend=lastquarterend.strftime('%Y-%m-%d')
        return (lastquarterend)

    #Pull a list of all funds in the database, the funds should 1.have NAV data on rebalday; 2.have holding data in the last quarterend before rebalday
    def GenerateFundlist(self,rebalday):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        sql1="select convert(varchar,NAV_date,23), code from mutual_fund_performance where NAV_date='"+rebalday+"'"
        reslist=ms.ExecQuery(sql1)    
        rechist=pd.DataFrame(reslist,columns=['date','ticker'])
        tickerlist=list(rechist['ticker'].unique())
        lastquarterend=self.lastquarterend(rebalday)
        sql2="select convert(varchar,NAV_date, 23), code from mutual_fund_performance where NAV_date='"+lastquarterend+"'"
        reslist2=ms.ExecQuery(sql2)    
        rechist2=pd.DataFrame(reslist2,columns=['date','ticker'])
        tickerlist=list(set(rechist['ticker'])& set(rechist2['ticker']))
        return(tickerlist)
        
    #Download the DTD performance of funds
    def FundPerf_History(self):
        #tickerlist=self.GenerateFundlist()
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        #lastyeardate=datetime.datetime.now()-relativedelta(years=1)
        #lastyeardate=lastyeardate.strftime('%Y-%m-%d')
        #sql="select NAV_date, code, dtd_change from mutual_fund_performance where NAV_date>'"+lastyeardate+"'"
        sql="select convert(varchar,NAV_date,23), code, dtd_change from mutual_fund_performance where NAV_date>'2015-01-01' and code like '%.OF%'"
        reslist=ms.ExecQuery(sql)    
        rechist=pd.DataFrame(reslist,columns=['date','fundticker','dailyreturn'])
        rechist['date']=[s[0:10]for s in rechist['date']]
        rechist=rechist.dropna()
        rechist.to_csv("D:/SecR/FundDaily.csv",index=False)
        return(rechist)
    
    #Supplement the fundperformance from last downloaded history
    def FundPerf(self):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        histfundperf=pd.read_csv("D:/SecR/FundDaily.csv")
        maxday=histfundperf['date'].max()
        sql="select convert(varchar,NAV_date,23), code, dtd_change from mutual_fund_performance where NAV_date>'"+maxday+"' and code like '%.OF%'"
        #sql="select NAV_date, code, dtd_change from mutual_fund_performance where NAV_date>'"+lastyeardate+"' and code in ("+str(tickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)    
        rechist=pd.DataFrame(reslist,columns=['date','fundticker','dailyreturn'])
        if (rechist.shape[0]>0):
            rechist['date']=[s[0:10]for s in rechist['date']]
            rechist=rechist.dropna()
            histfundperf=histfundperf.append(rechist)
            histfundperf.to_csv("D:/SecR/FundDaily.csv",index=False)
        return(histfundperf)

    #Download the holding of selected funds
    def TopHolding(self,tickerlist):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        lasttwoyeardate=datetime.datetime.now()-relativedelta(years=2)
        lasttwoyeardate=lasttwoyeardate.strftime('%Y-%m-%d')
        sql="select report_date,code, prt_topstockwindcode,stk_mkv_ratio from mutual_fund_holdings where report_date>'"+lasttwoyeardate+"' and code in ("+str(tickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)    
        rechist=pd.DataFrame(reslist,columns=['date','ticker','stockticker','portNav%'])
        rechist['date']=[s[0:10]for s in rechist['date']]
        rechist=rechist.dropna()
        rechist['portNav%']=rechist['portNav%']/100
        rechist['stockticker']=[s[0:6]for s in rechist['stockticker']]
        return(rechist)
   
    #Download indexreturn of sector index and the sector-mapping 
    def SectorPrep(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        sectormap=pd.read_csv('D:/SecR/sector_map.csv')
        sectormap['sector']=sectormap['sector'].astype(str)
        sectormap['ticker']=sectormap['ticker'].astype(str)
        sectormap['sector']=[x.zfill(2) for x in sectormap['sector']]
        sectormap['ticker']=[x.zfill(6) for x in sectormap['ticker']]
        tickerlist=list(sectormap['ticker'].unique())
        sql="select convert(varchar,TradingDay,23), SM.SecuCode,  ChangePCT/100 from  JYDBBAK.dbo.QT_IndexQuote IQ left join  JYDBBAK.dbo.SecuMain SM on IQ.InnerCode=SM.InnerCode where  TradingDay>'2019-12-31' and SM.SecuCode in ("+str(tickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        indexreturn=pd.DataFrame(reslist,columns=['date','ticker','dailyreturn'])
        indexreturn=indexreturn.sort_values(by=['date'],ascending=[True])
        indexreturn['dailyreturn']=indexreturn['dailyreturn'].astype('float')
        return(indexreturn,sectormap)
   
    #Translate the stock holdings into sector holdings:
    def SecAlloc(self,df):
        tickerlist=list(df['stockticker'].unique())
        rebaldaylist=list(df['date'].unique())
        sec=DC.Stock_sectorCSI_CITIC(rebaldaylist,tickerlist)
        sec['index']=sec['date']+sec['ticker']
        df['index']=df['date']+df['stockticker']
        df=pd.merge(df,sec[['index','primecode']],on='index',how='left')
        df['sumnav']=df.groupby(['date','ticker'])['portNav%'].transform('sum')
        df['portNAV-adj']=df['portNav%']/df['sumnav']
        return(df)        
    
    #只有2000多个基金的Alpha records; 而且很多没有及时update
    def PM_Alpha_download(self,fundtickerlist):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        sql="select code,fund_fundmanager,fund_corp_fundmanagementcompany from MUTUAL_FUND_INFO where code in ("+str(fundtickerlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        pminfo=pd.DataFrame(reslist,columns=['fundticker','pm','fundcorp'])
        pmlist=list(pminfo['pm'].unique())
        pmlist=[x.encode('latin-1').decode('gbk') for x in pmlist]
        sql="select fund_fundmanager, fund_corp_fundmanagementcompany,fund_manager_totalreturnoverbenchmark from MUTUAL_FUND_manager where fund_fundmanager in ("+str(pmlist)[1:-1]+")"
        reslist=ms.ExecQuery(sql)
        pmoutperf=pd.DataFrame(reslist,columns=['pm','fundcorp','outperf'])
        pmoutperf['pm']=pmoutperf['pm']+pmoutperf['fundcorp']
        pmoutperf['pm']=[x.encode('latin-1').decode('gbk') for x in pmoutperf['pm']]
        pminfo['pm']=pminfo['pm']+pminfo['fundcorp']
        pminfo['pm']=[x.encode('latin-1').decode('gbk') for x in pminfo['pm']]
        pminfo=pd.merge(pminfo, pmoutperf,on=['pm'],how='left')
        return(pminfo)
        

class Analysis():
    def __init__(self):
        self.DC=FundDataCollect()
        
        
    #Run regression of a dataframe where returns of factors and funds are in it
    def Regression(self,df):
        colnames=set(list(df.columns))
        nonuse=set(['date','ticker','dailyreturn'])
        predictors=colnames.difference(nonuse)
        y=df['dailyreturn']
        x=df.loc[:,predictors]
        model=LinearRegression().fit(x,y)
        alpha=model.intercept_
        beta=model.coef_
        ablist=[alpha]
        ablist.extend(list(beta))
        return(ablist)     #Rsquare=model.score(x,y)
            
    #Downlaod fund P&L of the recent year and run regresssion against the factor premium, it gives a table of alpha and betas
    #Currently the APP page only shows the respective Alphas
    def Beta_General(self):
        factorpremium=pd.read_csv("D:/SecR/Facreturnhist_MixedCN.csv")
        factorpremium=factorpremium.loc[:,['date','Value_zscore','Quality_zscore','Growth_zscore','Size_zscore']]
        fundperf=self.DC.FundPerf()
        fundtickerlist=list(fundperf['fundticker'].unique())
        complist=[]
        i=0
        for fundticker in fundtickerlist:
            print (i)
            i=i+1
            fundpnl=fundperf.loc[fundperf['fundticker']==fundticker,:].copy()
            fundpnl=fundpnl.dropna()
            if ((fundpnl.shape[0])>180):
                #print(fundpnl.shape[0])
                fundpnl=fundpnl.sort_values(by=['date'],ascending=[True])
                fundpnl=pd.merge(fundpnl,factorpremium,on='date',how='left')
                fundpnl=fundpnl.dropna()
                if(fundpnl.shape[0]>180):
                    stockvariable=[fundticker]
                    ablist=self.Regression(fundpnl)
                    stockvariable.extend(ablist)
                    complist.append(stockvariable)
        comptab=pd.DataFrame(complist,columns=['fundticker','alpha','Value_beta','Quality_beta','Growth_beta','Size_beta'])
        return(comptab,fundperf)
        
    #Download Fundholding and Generate a Return curve by using the sector index P&L with the same PortNav%
    def Sector_skills(self,comptab):
        fundtickerlist=list(comptab['fundticker'].unique())
        fundholding=self.DC.TopHolding(fundtickerlist) 
        fundholding=self.DC.SecAlloc(fundholding)
        fundtickerlist=list(fundholding['fundticker'].unique())
        indexreturn,sectormap=self.DC.SectorPrep()
        sectormap.columns=['date','primecode','indexticker']
        today=date.today()
        fundholding=pd.merge(fundholding,sectormap[['primecode','indexticker']],on='primecode',how='left')
        caldays=pd.DataFrame(pd.date_range(fundholding['date'].min(),today.strftime("%Y-%m-%d"),freq='d'),columns=['date'])
        caldays['date']=caldays['date'].astype(str)
        tradingday=pd.DataFrame(list(indexreturn.loc[indexreturn['date']>=fundholding['date'].min(),'date'].unique()),columns=['date'])
        indexreturn['index']=indexreturn['date']+indexreturn['ticker']
        secrotate=pd.DataFrame()
        for fundticker in fundtickerlist:
            print(fundticker)
            holdinghist=fundholding.loc[fundholding['ticker']==fundticker,:].copy()
            if (holdinghist.shape[0]>20)&(holdinghist.loc[holdinghist['indexticker'].isnull()==False,:].shape[0]>20):
                holdinghist['index']=holdinghist['date']+holdinghist['primecode']
                holdinghist['secnav']=holdinghist.groupby(['index'])['portNAV-adj'].transform('sum')
                fundholdinghist=holdinghist.pivot_table(index='date',columns='indexticker',values='secnav',aggfunc='first')
                fundholdinghist.reset_index(inplace=True)
                fundholdinghist=fundholdinghist.fillna(0)
                fundholdinghist=pd.merge(caldays,fundholdinghist,on='date',how='left')
                fundholdinghist=fundholdinghist.fillna(method='ffill')
                fundholdinghist=pd.merge(tradingday,fundholdinghist,on='date',how='left')
                fundholdinghist=pd.melt(fundholdinghist,id_vars='date',value_vars=list(fundholdinghist.columns[1:len(fundholdinghist.columns)+1]),var_name='indexticker',value_name='indexalloc')
                fundholdinghist=fundholdinghist.loc[fundholdinghist['indexalloc']>0,:].copy()
                fundholdinghist['index']=fundholdinghist['date']+fundholdinghist['indexticker']
                fundholdinghist=pd.merge(fundholdinghist,indexreturn[['index','dailyreturn']],on='index',how='left')
                fundholdinghist['weighteddaily']=fundholdinghist['indexalloc']*fundholdinghist['dailyreturn']
                fundholdinghist['sector_return']=fundholdinghist.groupby(['date'])['weighteddaily'].transform('sum')
                fundholdinghist=fundholdinghist.drop_duplicates(subset=['date'],keep='last')
                fundholdinghist['fundticker']=fundticker    
                secrotate=secrotate.append(fundholdinghist)
        secrotate=secrotate[['date','fundticker','sector_return']]
        return(secrotate,fundholding)
    
    #Return the P&L of each fund, if it doesn't rebalance the portfolio intra-quarter. This is their stockpick return
    def Stockpick_skills(self,fundholding,dailyreturn):
        fundtickerlist=list(fundholding['ticker'].unique())
        stocktickerlist=list(fundholding['stockticker'].unique())
        today=date.today()
        caldays=pd.DataFrame(pd.date_range(fundholding['date'].min(),today.strftime("%Y-%m-%d"),freq='d'),columns=['date'])
        caldays['date']=caldays['date'].astype(str)
        stockreturn=dailyreturn.loc[(dailyreturn['ticker'].isin(stocktickerlist))&(dailyreturn['date']>=caldays['date'].min()),:].copy()
        stockreturn['index']=stockreturn['date']+stockreturn['ticker']
        tradingday=pd.DataFrame(list(stockreturn['date'].unique()),columns=['date'])
        stockpick=pd.DataFrame()
        for fundticker in fundtickerlist:
            holdinghist=fundholding.loc[fundholding['ticker']==fundticker,:].copy()
            fundholdinghist=holdinghist.pivot_table(index='date',columns='stockticker',values='portNAV-adj',aggfunc='first')
            fundholdinghist.reset_index(inplace=True)
            fundholdinghist=fundholdinghist.fillna(0)
            fundholdinghist=pd.merge(caldays,fundholdinghist,on='date',how='left')
            fundholdinghist=fundholdinghist.fillna(method='ffill')
            fundholdinghist=pd.merge(tradingday,fundholdinghist,on='date',how='left')
            fundholdinghist=pd.melt(fundholdinghist,id_vars='date',value_vars=list(fundholdinghist.columns[1:len(fundholdinghist.columns)+1]),var_name='stockticker',value_name='portNAV-adj')
            fundholdinghist=fundholdinghist.loc[fundholdinghist['portNAV-adj']>0,:].copy()
            fundholdinghist['index']=fundholdinghist['date']+fundholdinghist['stockticker']
            fundholdinghist=pd.merge(fundholdinghist,stockreturn[['index','dailyreturn']],on='index',how='left')
            fundholdinghist['weighteddaily']=fundholdinghist['portNAV-adj']*fundholdinghist['dailyreturn']
            fundholdinghist['stock_return']=fundholdinghist.groupby(['date'])['weighteddaily'].transform('sum')
            fundholdinghist=fundholdinghist.drop_duplicates(subset=['date'],keep='last')
            fundholdinghist['fundticker']=fundticker
            stockpick=stockpick.append(fundholdinghist)
        stockpick=stockpick[['date','fundticker','stock_return']]
        return(stockpick)
    
    def FundGentable(self,comptab,fundperf,dailyreturn):
        #comptab,fundperf=self.Beta_General()
        secrotate,fundholding=self.Sector_skills(comptab)
        stockreturn=self.Stockpick_skills(fundholding,dailyreturn)
        tickerlist1=set(list(comptab['fundticker'].unique()))
        tickerlist2=set(list(secrotate['fundticker'].unique()))
        tickerlist3=set(list(stockreturn['fundticker'].unique()))
        fundtickerlist=(tickerlist1,tickerlist2,tickerlist3)#take the intersection of all three data source
        stockattributetab=pd.DataFrame()
        for fundticker in fundtickerlist:
            fundreturn=fundperf.loc[fundperf['fundticker']==fundticker,:].copy()
            fundsecreturn=secrotate.loc[secrotate['fundticker']==fundticker,:].copy()
            fundstockreturn=stockreturn.loc[stockreturn['fundticker']==fundticker,:].copy()
            fundreturn=pd.merge(fundreturn,fundsecreturn[['date','sector_return']],on='date',how='left')
            fundreturn=pd.merge(fundreturn,fundstockreturn[['date','stock_return']],on='date',how='left')
            fundreturn=fundreturn.sort_values(by=['date'],ascending=[True])
            fundreturn['seccumreturn']=np.exp(np.log1p(fundreturn['sector_return']).cumsum())
            fundreturn['stockcumreturn']=np.exp(np.log1p(fundreturn['stock_return']).cumsum())
            fundreturn['fundcumreturn']=np.exp(np.log1p(fundreturn['dailyreturn']).cumsum())
            fundreturn['tradingreturn']=fundreturn['fundcumreturn']-fundreturn['stockcumreturn']
            fundreturn['stockpickingreturn']=fundreturn['stockcumreturn']-fundreturn['seccumreturn']
            stockattributetab.append(fundreturn.loc[fundreturn.shape[0]-1,['ticker','seccumreturn','stockcumreturn','fundcumreturn','stockpickingreturn','tradingreturn']])
        comptab=pd.merge(comptab,stockattributetab,on='ticker',how='left')
        return(comptab)
    
    
class Shen6_Auto():
    def __init__(self):
        self.DC=FundDataCollect()

    
    #Pick the fund with latest holding update within 4months
    def Pick_fund_holding(self,ref):
        lastupdate=self.DC.lastquarterend(ref)
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        sql="select convert(varchar,report_date,23),code,id, prt_topstockwindcode,stk_mkv_ratio from mutual_fund_holdings where report_date='"+lastupdate+"'"
        reslist=ms.ExecQuery(sql)    
        rechist=pd.DataFrame(reslist,columns=['date','fundticker','fundid','ticker','portNav%'])
        rechist['sum%']=rechist.groupby('fundticker')['portNav%'].transform('sum')
        rechist=rechist.loc[rechist['sum%']>=50,:].copy()                           #只保留前10持仓之和超过50的，不然没有抄作业的价值
        return(rechist)
    
    
        
    #from the shen_six_holding download the selected fund on the last updated date #直接调用数据库里的神六纪录；算法由恒恒maintained
    #extract the lastest holding records of these funds 
    def Hengheng_Shen6Holding(self):
        ms = MSSQL(host="10.27.10.10:1433",user="hyzb",pwd="hyzb2018",db="hyzb") 
        sql="select convert(varchar,date,23), code from shen_six_holding SIX where date in (select Max(date) from shen_six_holding SIX)"
        fundlist=ms.ExecQuery(sql)  
        selectedfund=pd.DataFrame(fundlist,columns=['date','fundticker']) 
        fundlist=list(selectedfund['fundticker'].unique())
        lastholding=self.Pick_fund_holding()
        selectedfundholding=lastholding.loc[lastholding['fundticker'].isin(fundlist),:].copy()
        return(selectedfundholding)
 
    
class NewStructure():
    def __init__(self):
        self.DC=FundDataCollect()
        self.S6=Shen6_Auto()
        
        #Generate portNAV based on marketcap, with 15% cap on stocks
    def PortNavMcap(self,postab,dailyreturn):
        daylist=list(postab['date'].unique())
        mcaptab=dailyreturn.loc[(dailyreturn['date'].isin(daylist))&(dailyreturn['ticker'].isin(postab['ticker'].unique())),['date','ticker','mcap']].copy()
        mcaptab['index']=mcaptab['date']+mcaptab['ticker']
        postab['index']=postab['date']+postab['ticker']
        postab=pd.merge(postab,mcaptab[['index','mcap']],on=['index'],how='left')
        postab['totalMcap']=postab.groupby(['date'])['mcap'].transform('sum')
        postab['PortNav%']=postab['mcap']/postab['totalMcap']
        postab=postab.dropna()
        postabfinal=pd.DataFrame()
        for day in daylist:
            postab_temp=postab.loc[postab['date']==day,:].copy()
            while (postab_temp['PortNav%'].max()>0.15):
                postab_temp=self.Generate_PortNavMcap_CAP(postab_temp,dailyreturn)
            postabfinal=postabfinal.append(postab_temp)
        postabfinal=postabfinal[['date','ticker','PortNav%']]
        return(postabfinal)
    
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
    
    #Adjusted the marketcap weighted % according to scores (shen5=1/shen6=1/hotstock=1)
    def Score_adj_PortNav(self,df):
        df['newportnav']=df['PortNav%']
        df.loc[df['score']==3,'newportnav']=df.loc[df['score']==3,'PortNav%']*1.2
        df.loc[df['score']==2,'newportnav']=df.loc[df['score']==2,'PortNav%']*1.1
        df['addednav']=df['newportnav']-df['PortNav%']
        df['sumaddednav']=df.groupby(['date'])['addednav'].transform('sum')
        df['posnum']=df.groupby(['date'])['index'].transform('count')
        df['averagedown']=df['sumaddednav']/df['posnum']
        df.loc[df['score']<2,'newportnav']=df.loc[df['score']<2,'PortNav%']-df.loc[df['score']<2,'averagedown']
        df['PortNav%']=df['newportnav']
        df=df[['date','ticker','PortNav%']].copy()
        df=df.sort_values(by=['date','PortNav%'],ascending=[True,False])
        return(df)
        
        
    def Hotstock(self,dailyreturn,rebaldaylist,topn):
        hotstock=HSR.ActivepickNS_production(dailyreturn,rebaldaylist,topn)
        Ahotstock=hotstock.loc[hotstock['ticker'].str.len()>=6,:].copy()
        HKhotstock=hotstock.loc[hotstock['ticker'].str.len()<6,:].copy()
        return(Ahotstock,HKhotstock)
    
    #Rank the latest 3month maxdd, ann_sharpe, ann_return among available funds. 
    def MyShen6_Update(self,rebaldaylist,histfundperf,dailyreturn):
        #histfundperf=self.DC.FundPerf()
        histnfelite=pd.DataFrame()
        tradingday=pd.DataFrame(dailyreturn['date'].unique(),columns=['date'])
        for rebalday in rebaldaylist:
            print(rebalday)
            threemonthsago=pd.to_datetime(rebalday)-relativedelta(months=3)
            threemonthsago=threemonthsago.strftime('%Y-%m-%d')
            activefund=list(histfundperf.loc[histfundperf['date']==rebalday,'fundticker'].copy())  #一定要在最近一个perf update日有DTD记录;Active funds
            activefundhist=histfundperf.loc[(histfundperf['fundticker'].isin(activefund))&(histfundperf['date']>=threemonthsago)&(histfundperf['date']<=rebalday),:].copy() #截取最近3个月的Dailyperf
            lastholding=self.S6.Pick_fund_holding(rebalday)                                               #下载最近一个持仓更新日的持仓信息
            holdingfund=list(lastholding['fundticker'].unique())
            activefundlist=list(set(activefund) & set(holdingfund))                            #一定要在最近一个holding update日有holding信息           
            datalength=len(tradingday.loc[(tradingday['date']<=rebalday)&(tradingday['date']>=threemonthsago),'date']) #how many trading days in recent 3months
            featuretab=[]
            #pmalpha=self.DC.PM_Alpha_download(activefundlist)
            for fundticker in activefundlist:
                featurelist=[]
                singlefundhist=activefundhist.loc[activefundhist['fundticker']==fundticker,:].copy()
                if (singlefundhist.shape[0]>(datalength*0.9))&(singlefundhist['dailyreturn'].isnull().any()==False): #DTD return的数据，至少要有总交易日数的90%。缺失值不能超过10%
                    #print(fundticker)
                    singlefundhist=singlefundhist.sort_values(by=['date'],ascending=[True])
                    maxdd=em.max_drawdown(singlefundhist['dailyreturn'])
                    sharpe=em.sharpe_ratio(singlefundhist['dailyreturn'])
                    irr=em.annual_return(singlefundhist['dailyreturn'])
                    featurelist.extend([fundticker,maxdd,sharpe,irr])
                    featuretab.append(featurelist)
            featuretab=pd.DataFrame(featuretab)
            featuretab.columns=['fundticker','maxdd','ann_sharpe','ann_return']
            for metric in ['maxdd','ann_sharpe','ann_return']:
                featuretab[metric+'_rank']=featuretab[metric].rank(method="dense", ascending=False) #值越大排名越低
            nf=pd.merge(featuretab,lastholding[['fundticker','ticker','portNav%']],on='fundticker',how='left')
            #nf=pd.merge(nf,pmalpha[['fundticker','outperf']],on='fundticker',how='left')
            nf=nf.dropna()
            nf['stocktotalsum']=nf.groupby(['ticker'])['portNav%'].transform('sum')
            nf['General_Rank']=nf['maxdd_rank']*0.4+nf['ann_sharpe_rank']*0.3+nf['ann_return_rank']*0.3          #计算总分,排名越低越好
            nf['Rank_of_Rank']=nf['General_Rank'].rank(method="dense", ascending=False)      #越小排名越低  
            nfelite=nf.loc[nf['Rank_of_Rank']<=50,:].copy()
            nfelite['date']=rebalday
            histnfelite=histnfelite.append(nfelite)
        histnfelite=histnfelite.loc[histnfelite['ticker'].str.len()==9,:].copy()            #Include Ashs
        histnfelite['ticker']=[x[0:6] for x in histnfelite['ticker']]
        return(histnfelite)
    
    #Generate the list of Shen5 3+4 (historical)
    def Shen5(self,rebaldaylist,dailyreturn):
        benchmark='CSIAll'
        lookback_period=60
        N2,Sec,NS,HS=AR.TApostab(dailyreturn,rebaldaylist,lookback_period,benchmark)
        Shen5=pd.DataFrame()
        Shen5=Shen5.append([N2[['date','ticker']],Sec[['date','ticker']],NS[['date','ticker']],HS[['date','ticker']]])
        Shen5['index']=Shen5['date']+Shen5['ticker']
        Shen5['count']=Shen5.groupby('index')['index'].transform('count')
        Shen5=Shen5.drop_duplicates(subset=['index'],keep='first')
        Shen5=Shen5.loc[Shen5['count']>=3].copy()
        return(Shen5)
    
    #Combine Shen5 & Shen6, carry the score (1 from Shen5 and 1 from Shen6)
    def Shen56_Pool(self,rebaldaylist,dailyreturn):
        histfundperf=self.DC.FundPerf()
        nfelite=self.MyShen6_Update(rebaldaylist,histfundperf,dailyreturn)
        shen5=self.Shen5(rebaldaylist,dailyreturn)
        nfelite['index']=nfelite['date']+nfelite['ticker']
        nfelite=nfelite.drop_duplicates(subset=['index'],keep='first')
        shen56pool=shen5[['date','ticker','index']].append(nfelite[['date','ticker','index']])
        shen56pool['count']=shen56pool.groupby('index')['index'].transform('count')
        shen56pool=shen56pool.drop_duplicates(subset=['index'],keep='first')
        shen56pool=shen56pool.sort_values(by=['date'],ascending=[True])
        return(shen56pool)
    
    #use DuBin's Shen6 & Old Shen5 combined instead of generating Shen6 from SQL database
    def Shen56_Pool_Hist(self,rebaldaylist,dailyreturn):
        #shen5=self.Shen5(rebaldaylist,dailyreturn)
        #shen6hist=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
        #shen6hist['ticker']=[x[0:6] for x in shen6hist['ticker']]
        #shen6hist=DS.Rebalday_alignment(shen6hist,rebaldaylist)
        #shen6hist['index']=shen6hist['date']+shen6hist['ticker']
        #shen6hist=shen6hist.drop_duplicates(subset=['index'],keep='first')
        shen56pool=pd.read_csv("D:/SecR/Shen56New.csv")
        shen56pool['ticker']=shen56pool['ticker'].astype(str)
        shen56pool['ticker']=[x.zfill(6)for x in shen56pool['ticker']]
        shen56pool=DS.Rebalday_alignment(shen56pool,rebaldaylist)
        shen56pool['index']=shen56pool['date']+shen56pool['ticker']
        #shen56pool=shen5[['date','ticker','index']].append(shen6hist[['date','ticker','index']])
        shen56pool['count']=shen56pool.groupby('index')['index'].transform('count')
        shen56pool=shen56pool.drop_duplicates(subset=['index'],keep='first')
        shen56pool=shen56pool.sort_values(by=['date'],ascending=[True])
        return(shen56pool)
    
    def S567(self,dailyreturn,rebaldaylist,shen56pool):
        #shen56pool=self.Shen56_Pool(rebaldaylist,dailyreturn)
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        #Use the shen56pool as a small unvierse to calculate Qs
        Qtab_smalluniverse=pd.DataFrame()
        fzdict=FF.Fzdict(dailyreturn,rebaldaylist,facdict,shen56pool,'ThreeFour') #Use the shen56pool as universe
        fztab=FF.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        for rebalday in rebaldaylist:
                   rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
                   rebalz['rank']=rebalz['meanscore'].rank(method='first')
                   rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
                   Qtab_smalluniverse=Qtab_smalluniverse.append(rebalz)
        #Use the top70%market as a large universe to calculate Qs
        Qtab_largeuniverse=pd.DataFrame()       
        fzdict=FF.Fzdict(dailyreturn,rebaldaylist,facdict,shen56pool,'N') #Use the wholemarket as universe
        fztab=FF.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        for rebalday in rebaldaylist:
                   rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
                   rebalz['rank']=rebalz['meanscore'].rank(method='first')
                   rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
                   Qtab_largeuniverse=Qtab_largeuniverse.append(rebalz)
        Qtab_smalluniverse['index']=Qtab_smalluniverse['date']+Qtab_smalluniverse['ticker']
        Qtab_smalluniverse4=Qtab_smalluniverse.loc[Qtab_smalluniverse['Q']>3,:].copy()
        Qtab_largeuniverse['index']=Qtab_largeuniverse['date']+Qtab_largeuniverse['ticker']
        Qtab_largeuniverse4=Qtab_largeuniverse.loc[Qtab_largeuniverse['Q']>3,:].copy()
        Qtab_core=Qtab_smalluniverse4.loc[Qtab_smalluniverse4['index'].isin(Qtab_largeuniverse4['index']),:].copy() #Only stocks that are in Q4/Q5 in both universes will be shortlisted
        return(Qtab_core)
    
    #S7 on each sector alone
    def S7(self,dailyreturn,rebaldaylist,sectorpool):
        facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        #Use the shen56pool as a small unvierse to calculate Qs
        Qtab_largeuniverse=pd.DataFrame()       
        fzdict=FF.Fzdict(dailyreturn,rebaldaylist,facdict,sectorpool,'ThreeFour') #Use the wholemarket as universe
        fztab=FF.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        for rebalday in rebaldaylist:
                   rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
                   rebalz['rank']=rebalz['meanscore'].rank(method='first')
                   rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
                   Qtab_largeuniverse=Qtab_largeuniverse.append(rebalz)
        Qtab_largeuniverse['index']=Qtab_largeuniverse['date']+Qtab_largeuniverse['ticker']
        Qtab_core=Qtab_largeuniverse.loc[Qtab_largeuniverse['Q']>3,:].copy()
        return(Qtab_core)
    
    #Take out all the FIG stocks, keep the nonFIG the same as CSI300's proportion and supplement it with FIG hotstocks
    def FIG_restructure(self,df,dailyreturn):
        df,bm_sector_rebalday=WS.Benchmark_sector_weight(df,'CSI300')                          #Give the CSI300's weighting of stocks' sector (with special treament of FIG)
        bmo=bm_sector_rebalday.copy()
        bm_sector_rebalday=bm_sector_rebalday.loc[bm_sector_rebalday['secondcode'].isin(['40','41','42','4120']),:].copy()
        bm_sector_rebalday['BMFISecweight']=bm_sector_rebalday.groupby(['date'])['sector%'].transform('sum')
        bm_sector_rebalday['BMFISecweight']=bm_sector_rebalday['BMFISecweight']*0.7            #保证金融股比重为CSI300金融类比重的70%
        bm_sector_rebalday['BMNFISecweight']=1-bm_sector_rebalday['BMFISecweight']
        bmo['sector%']=bmo['sector%']*0.7                                                      #保证金融股比重为CSI300金融类比重的70%
        rebaldaylist=list(df['date'].unique())
        hotFIG=HSR.TopHotFIG(dailyreturn,rebaldaylist)
        hotFIG['index']=hotFIG['date']+hotFIG['secondcode']
        df['FIG']='N'
        df.loc[df['secondcode'].isin(['40','41','42','4120']),'FIG']='Y'
        df['index']=df['date']+df['FIG']
        df['NFIGweight']=df.groupby(['index'])['PortNav%'].transform('sum')                     #postab里原来属于FIG/不属于FIG的分别%是多少
        df['index']=df['date']+df['secondcode']
        df=pd.merge(df,bm_sector_rebalday[['date','BMNFISecweight']],on='date',how='left')
        df=df.drop_duplicates()
        dfNF=df.loc[df['FIG']=='N',:].copy()
        dfNF['PortNav%']=dfNF['PortNav%']/dfNF['NFIGweight']                                   #非金融股内部调整到100%
        dfNF['PortNav%']=dfNF['PortNav%']*dfNF['BMNFISecweight']                               #非金融谷总体乘以当时BM非金融股票的%
        hotFIG=pd.merge(hotFIG,bmo[['index','sector%']],on='index',how='left')
        hotFIG['count']=hotFIG.groupby(['index'])['index'].transform('count')                  #热门金融股和BM的金融股各细分行业百分比合并
        hotFIG['PortNav%']=hotFIG['sector%']/hotFIG['count']                                   #热门金融股个股%为BM的金融股个细分行业均分
        dfNF=dfNF[['date','ticker','PortNav%']]
        hotFIG=hotFIG[['date','ticker','PortNav%']]
        dfNF=dfNF.append(hotFIG)
        dfNF=dfNF.sort_values(by=['date'],ascending=[True])
        return(dfNF)
        
    def S567Hot(self,rebaldaylist,dailyreturn):
        topn=50
        shen56pool=self.Shen56_Pool(rebaldaylist,dailyreturn)
        Qtab_core=self.S567(dailyreturn,rebaldaylist,shen56pool)
        Ahot,HKhot=self.Hotstock(dailyreturn,rebaldaylist,topn)
        Ahot['index']=Ahot['date']+Ahot['ticker']
        tabS567Hot=Qtab_core[['date','ticker','index']].append(Ahot[['date','ticker','index']])
        tabS567Hot=tabS567Hot.drop_duplicates(subset=['index'],keep='first')
        postab=self.PortNavMcap(tabS567Hot,dailyreturn)
        postab=self.FIG_restructure(postab,dailyreturn)
        PNL=RC.DailyPNL(dailyreturn,postab)
        PNL['CumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        #rechist_bm=WS.Benchmark_intersect(tabS567Hot,'CSI300')
        #rechist_bm=rechist_bm.sort_values(by=['date','weight'],ascending=[True,False])
        #postab=WS.Generate_PortNav(rechist_bm)
        #PNL=RC.DailyPNL(dailyreturn,postab)
        #PNL['CumReturn']=np.exp(np.log1p(PNL['dailyreturn']).cumsum())
        return(postab,PNL,tabS567Hot)
        
    
 
        #postab=self.PortNavMcap(tabS567Hot,dailyreturn)


#用旧方法还原历史数据
#旧方法里2021前的shen6数据由杜彬那里来
class Hist_Data_Generation():
    def __init__(self):
        self.FDC=FundDataCollect()
        self.S6=Shen6_Auto()
        self.NS=NewStructure()
    
    def S567_Original(self):
        dailyreturn=DC.Dailyreturn_retrieve()
        histrebaldaylist=DC.Rebaldaylist('2016-01-04',60)
        shen6rebaldaylist=[x for x in histrebaldaylist if x>'2020-07-31']
        shen5=self.NS.Shen5(histrebaldaylist, dailyreturn)
        shen5=shen5[['date','ticker']].copy()
        shen6_old=pd.read_csv("D:/SecR/Shen6_Stocks.csv")
        shen6_old['ticker']=[x[0:6]for x in shen6_old['ticker']]
        shen6_old=DS.Rebalday_alignment(shen6_old,histrebaldaylist)
        histfundperf=self.FDC.FundPerf()
        shen6_new=self.NS.MyShen6_Update(shen6rebaldaylist,histfundperf,dailyreturn)
        shen6_new=shen6_new[['date','ticker']]
        shen6_new['index']=shen6_new['date']+shen6_new['ticker']
        shen6_new=shen6_new.drop_duplicates(subset=['index'])
        shen6_new=shen6_new[['date','ticker']]
        shen6=shen6_old.append(shen6_new)
        CSIAll=DC.Benchmark_membs('CSIAll','2011-01-04')
        CSIAll=CSIAll[['date','ticker']].copy()
        newA=DS.Rebalday_alignment(CSIAll,histrebaldaylist)
        shen7=self.NS.S567(dailyreturn, histrebaldaylist, newA)
        shen7=shen7.loc[shen7['Q']==5,['date','ticker']].copy()
        gen_stockpool=shen5.append([shen6,shen7])
        gen_stockpool=gen_stockpool.drop_duplicates()
        gen_stockpool['ticker']=[x+' CH' for x in gen_stockpool['ticker']]
        return(gen_stockpool)
        
    #For Non-SuperTrend/Non-FIG sectors: 1.Get Shen56/2. If stocks>20=>Shen7Top2Q/3. combine with HotStock (top30%)=>MarketCap weighted with 15%cap
    #For FIG: Just use Top30% Hotstocks=>marketcap weighted
    #For SuperTrend: don't need to use this program to build model portfolio
    #To be Done: use Shen56/Shen7/Hotstocks to re-adjusted marketcap based weighting 
    def S567Sectors(self,rebaldaylist,dailyreturn):
        allstocks=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),['date','ticker']].copy()
        allstocks=WS.Stock_sectorMixed(allstocks)
        allstocks=allstocks.dropna()
        allstocks.loc[allstocks['primecode'].isin(['4110','4120','4130','42']),'primecode']='41'    #All nonbank subfinancials are consolidated=>NonBank 41
        sectorlist=list(allstocks['primecode'].unique())
        #shen56pool=self.NS.Shen56_Pool(rebaldaylist,dailyreturn)                               #Generate Shen56 pool
        shen56pool=self.NS.Shen56_Pool_Hist(rebaldaylist,dailyreturn)
        shen56pool['index']=shen56pool['date']+shen56pool['ticker']
        allstocks['index']=allstocks['date']+allstocks['ticker']
        shen56pool=pd.merge(shen56pool,allstocks[['index','primecode']],on='index',how='left') #Shen56pool + sector primecode
        HotStocks_Allsector=HSR.ActivepickBMSec(dailyreturn,'no',rebaldaylist)                 #Top30% of stocks of every sector
        portdict={}
        sectorlist=[x for x in sectorlist if x not in ['04','05','07']]                        #Exclude MegaTrend sectors
        for sector in sectorlist:
            print(sector)
            shen56poolsector=shen56pool.loc[shen56pool['primecode']==sector,:].copy()          #Shen56pool of this sector
            if ((shen56poolsector.shape[0]/len(rebaldaylist))>20) and (sector not in (['04','05','07','40','41','42'])): 
                Qtab_core=self.NS.S7(dailyreturn,rebaldaylist,shen56poolsector)                #Shen7 exclude parts of it
                sector_shortlisted=Qtab_core[['date','ticker']].copy()
                sector_shortlisted['index']=sector_shortlisted['date']+sector_shortlisted['ticker']
                sector_shortlisted=pd.merge(sector_shortlisted,shen56poolsector[['index','count']],on='index',how='left')
            else:
                sector_shortlisted=pd.DataFrame()
            Hotstocks_sector=HotStocks_Allsector.loc[HotStocks_Allsector['sector']==sector,:].copy() #Combined with Hotstocks
            Hotstocks_sector['racrank']=Hotstocks_sector.groupby(['date'])['raccount'].rank("dense", ascending=False)
            Hotstocks_sector=Hotstocks_sector.loc[Hotstocks_sector['racrank']<=10,:]                 #Choose only top10 /if real estate (42) choose only top 5
            Hotstocks_sector=Hotstocks_sector[['date','ticker']].copy()  
            Hotstocks_sector['count']=1                            
            sector_shortlisted=sector_shortlisted.append(Hotstocks_sector)                           #Combined Shen567 and Hotstocks
            sector_shortlisted['index']=sector_shortlisted['date']+sector_shortlisted['ticker']
            sector_shortlisted['score']=sector_shortlisted.groupby(['index'])['count'].transform('sum')
            sector_shortlisted=sector_shortlisted.drop_duplicates(subset=['index'])
            sectorport=self.NS.PortNavMcap(sector_shortlisted,dailyreturn)
            sectorport['index']=sectorport['date']+sectorport['ticker']
            sectorport=pd.merge(sectorport,sector_shortlisted[['index','score']],on='index',how='left') #Score Shen5=1/Shen6=1/Hotstock=1 
            sectorport=self.NS.Score_adj_PortNav(sectorport)
            sectorport=sectorport.loc[sectorport['PortNav%']>0,:].copy()
            sectorport['ticker']=[x+' CH' for x in sectorport['ticker']]
            sectorport.to_csv("D:/MISC/"+str(sector)+".csv",index=False)
            sectorport['ticker']=[x[0:6] for x in sectorport['ticker']]
            portdict[sector]=sectorport
        portPNL={k:RC.DailyPNL(dailyreturn,v)for k,v in portdict.items()}
        for sector in portPNL.keys():
            dailypnl=portPNL[sector]
            dailypnl['cumPNL']=np.exp(np.log1p(dailypnl['dailyreturn']).cumsum())
            dailypnl.to_csv("D:/MISC/"+str(sector)+"cumPNL.csv",index=False)
        return(portdict)
        
        
        