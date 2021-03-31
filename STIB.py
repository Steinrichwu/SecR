# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 19:34:47 2021

@author: wudi
"""
#科创板专用

from MSSQL import MSSQL
import pandas as pd
import numpy as np
import statsmodels.api as sm
from Toolbox import DataStructuring 
from scipy import stats
from datetime import datetime, timedelta, date
from functools import reduce
from HotStock import Prep as HSPrep
DS=DataStructuring()

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")

class DataCollect():
    def __init__(self):
            pass
        
    def Hotstock(self,rebalday):
        lookback_period=60
        ms2 = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="jyzb_new_1") #This is PROD   
        sql="Select '"+rebalday+"' as date, code, count(*) Reccount from jyzb_new_1.dbo.cmb_report_research R left join jyzb_new_1.dbo.I_SYS_CLASS C on C.SYS_CLASS=R.score_id left join jyzb_new_1.dbo.I_ORGAN_SCORE S on S.ID=R.organ_score_id where into_date>=dateadd(day,-"+str(lookback_period)+",'"+rebalday+"') and into_date<'"+rebalday+"' and (sys_class=7 OR sys_class=5) GROUP BY code ORDER BY Reccount DESC "
        reslist=ms2.ExecQuery(sql)
        rechist=pd.DataFrame(reslist,columns=['date','ticker','RecomCounts'])
        return(rechist)
    
    def Growth(self,df):
        df['month']=df['enddate'].str[5:7]
        df['index']=df['ticker']+df['signame']+df['month']
        df=df.sort_values(by=['ticker','index','enddate'],ascending=[True,True,True])
        df['occurence']=df.groupby('index').cumcount()+1
        df['growth']=(df['sigvalue']-df['sigvalue'].shift(1))/abs(df['sigvalue'].shift(1))
        df.loc[df['occurence']==1,'growth']=0
        return(df)
    
    def Valuation(self,startdate,dfMarket):
        query="Select TradingDay, SM.SecuCode, PELYR, PB, PS, NegotiableMV from JYDBBAK.dbo.LC_STIBDIndiForValue STIBI left join JYDBBAK.dbo.SecuMain SM on STIBI.InnerCode=SM.InnerCode where TradingDay='"+startdate+"'"
        reslist=ms.ExecQuery(query)
        dfST=pd.DataFrame(reslist,columns=['date','ticker','PE','PB','PS','Mcap'])
        query="Select TradingDay, SM.SecuCode, PELYR, PB, PS, NegotiableMV from JYDBBAK.dbo.LC_DIndicesForValuation DV left join JYDBBAK.dbo.SecuMain SM on DV.InnerCode=SM.InnerCode where TradingDay='"+startdate+"'"
        reslist=ms.ExecQuery(query)
        dfMain=pd.DataFrame(reslist,columns=['date','ticker','PE','PB','PS','Mcap'])
        df=dfST.append(dfMain)
        df[['PE','PB','PS']]=1/df[['PE','PB','PS']]
        df=df.loc[df['ticker'].isin(dfMarket['ticker']),:].copy()
        return(df)
    
    def Quality(self,dfMarket):
        query="Select SM.SecuCode, InfoPublDate, EndDate, ROETTM, ROATTM from JYDBBAK.dbo.LC_STIBMainIndex MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode where InfoPublDate>'2020-06-30' and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        dfST=pd.DataFrame(reslist,columns=['ticker','publdate','enddate','ROETTM','ROATTM'])
        query="Select SM.Secucode,QFI.InfoPublDate, QFI.EndDate, MI.ROETTM, MI.ROATTM from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>='2020-06-30'"
        reslist=ms.ExecQuery(query)
        dfMain=pd.DataFrame(reslist,columns=['ticker','publdate','enddate','ROETTM','ROATTM'])
        dfQuality=dfST.append(dfMain)
        dfQuality=dfQuality.loc[dfQuality['ticker'].isin(dfMarket['ticker']),:].copy()    
        dfQuality=dfQuality.sort_values(by=['ticker','publdate'],ascending=[True,True])
        dfQuality=dfQuality.drop_duplicates('ticker','last')
        return(dfQuality)
    
    def Revenue(self,dfMarket):
        query="Select SM.SecuCode, convert(varchar,ICS.InfoPublDate,23), convert(varchar,ICS.EndDate,23), ICS.OperatingRevenue,ICS.NPFromParentCompanyOwners from JYDBBAK.dbo.LC_QIncomeStatementNew ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCategory=1 AND ICS.Mark=2 and ICS.InfoPublDate>='2018-06-30'"
        reslist=ms.ExecQuery(query)
        dfMain=pd.DataFrame(reslist,columns=['ticker','publdate','enddate','QRevenue','QNetprofit'])
        query="Select SM.SecuCode, convert(varchar,ICS.InfoPublDate,23), convert(varchar,ICS.EndDate,23), ICS.OperatingRevenue, ICS.NPParentCompanyOwners from JYDBBAK.dbo.LC_STIBQIncomeState ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCategory=1 AND ICS.InfoPublDate>='2018-06-30'"
        reslist=ms.ExecQuery(query)
        dfSTBI=pd.DataFrame(reslist,columns=['ticker','publdate','enddate','QRevenue','QNetprofit'])
        dfRev=dfMain.append(dfSTBI)
        dfRev=pd.melt(dfRev,id_vars=['ticker','publdate','enddate'],value_vars=['QRevenue','QNetprofit'],var_name='signame',value_name='sigvalue')
        dfRev=dfRev.loc[dfRev['ticker'].isin(dfMarket['ticker']),:].copy()
        dfRev=dfRev.loc[(dfRev['sigvalue']!=0)|(dfRev['sigvalue'].isnull==False),:]
        dfRev['sigvalue']=dfRev['sigvalue'].astype(float)
        dfRevGrowth=self.Growth(dfRev)
        dfRevGrowth=dfRevGrowth.drop_duplicates('index','last')
        dfRevGrowth['index']=dfRevGrowth['ticker']+dfRevGrowth['signame']
        dfRevGrowth=dfRevGrowth.sort_values(by=['index','publdate'],ascending=[True,True])
        dfRevGrowth=dfRevGrowth.drop_duplicates('index','last')
        dfRevGrowth=dfRevGrowth.pivot_table(index=['ticker','publdate','enddate'],columns='signame',values='growth',aggfunc='first')        
        dfRevGrowth.reset_index(inplace=True)
        dfRevGrowth=dfRevGrowth.rename(columns={'QRevenue':'QRevenuegrowth'})
        dfRevGrowth=dfRevGrowth.rename(columns={'QNetprofit':'QNetprofitgrowth'})
        return(dfRevGrowth)
    
    def Market(self,startdate,dailyreturn):
        dfMain=dailyreturn.loc[dailyreturn['date']==startdate,['date','ticker','turnoverweek','dailyvolume']].copy()
        query="select convert(varchar,TradingDay, 23) as date, SM.SecuCode,TurnoverRateRW,TurnoverVolume from JYDBBAK.dbo.LC_STIBPerformance QTP left join JYDBBAK.dbo.SecuMain SM on QTP.InnerCode=SM.InnerCode where SM.SecuCategory = 1 and TradingDay='"+startdate+"'"
        reslist=ms.ExecQuery(query)
        dfSTBI=pd.DataFrame(reslist,columns=['date','ticker','turnoverweek','dailyvolume'])
        dfSTBI['turnoverweek']=1/dfSTBI['turnoverweek']
        dfMain['turnoverweek']=1/dfMain['turnoverweek']
        dfMarket=dfMain.append(dfSTBI)
        return(dfMarket)
    
    def Sec(self,dfMarket):
        query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_ExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        sec_name=pd.DataFrame(reslist,columns=['date','ticker','seccode'])
        sec_name=sec_name.sort_values(by=['ticker','date'],ascending=[True,True])
        sec_name=sec_name.drop_duplicates(subset=['ticker'],keep='last')
        query="SELECT convert(varchar,LEI.CancelDate,23),SecuCode,FirstIndustryCode FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_STIBExgIndustry LEI on SM.CompanyCode=LEI.CompanyCode where LEI.Standard=37 and SM.SecuCategory=1"
        reslist=ms.ExecQuery(query)
        sec_name2=pd.DataFrame(reslist,columns=['date','ticker','seccode'])
        sec_name2=sec_name2.sort_values(by=['ticker','date'],ascending=[True,True])
        sec_name2=sec_name2.drop_duplicates(subset=['ticker'],keep='last')
        sec_name=sec_name.append(sec_name2)
        sec_name=sec_name.loc[sec_name['ticker'].isin(dfMarket['ticker']),:].copy()
        return(sec_name)
    
class Analysis():
    def __init__(self):
         self.DC=DataCollect()
        
    def A_Merge(self,startdate,dailyreturn):
        dfMarket=self.DC.Market(startdate,dailyreturn)
        dfRevGrowth=self.DC.Revenue(dfMarket)
        dfQuality=self.DC.Quality(dfMarket)
        dfValue=self.DC.Valuation(startdate, dfMarket)
        dfsec=self.DC.Sec(dfMarket)
        data_frame=[dfMarket,dfRevGrowth[['ticker','QNetprofitgrowth','QRevenuegrowth']],dfQuality[['ticker','ROETTM','ROATTM']],dfValue[['ticker','PE','PB','PS','Mcap']],dfsec[['ticker','seccode']]]
        dfmerged=reduce(lambda left,right: pd.merge(left,right, on=['ticker'],how='left'),data_frame)
        dfmerged=dfmerged.drop_duplicates(subset=['ticker'],keep='last')
        return(dfmerged)
    
    def A_Analysis(self,df):
        indu_dummy=pd.get_dummies(df['seccode'])
        df=pd.concat([df,indu_dummy],axis=1)
        df=df.reset_index(drop=True)
        Xset=['Mcap']
        Xset.extend(indu_dummy.columns)
        selectsigs=['turnoverweek','QNetprofitgrowth','QRevenuegrowth', 'ROETTM', 'ROATTM', 'PE', 'PB', 'PS']
        df.iloc[:,2:]=df.iloc[:,2:].astype(float)
        dfO=df.copy()
        for sig in selectsigs:
            dfnona=dfO.loc[dfO[sig].isna()==False,:].copy()
            dfnona=DS.Winsorize(dfnona,sig,0.02)
            dfnona[sig]=dfnona[sig].astype(float)
            dfnona[Xset]=dfnona[Xset].astype(float)
            est=sm.OLS(dfnona[sig],dfnona[Xset]).fit()
            dfnona['N_'+sig]=est.resid.values
            df=pd.merge(df,dfnona[['ticker','N_'+sig]],on='ticker',how='left')
        df=df.drop(Xset,axis=1)
        dfO=df.copy()
        for sig in selectsigs:
            dfnona=dfO.loc[dfO['N_'+sig].isna()==False,:].copy()
            dfnona[sig+'_zscore']=stats.zscore(dfnona['N_'+sig])
            df=pd.merge(df,dfnona[['ticker',sig+'_zscore']],on='ticker',how='left')
        df['Quality_zscore']=(df['ROETTM_zscore']+df['ROATTM_zscore'])/2
        df['Growth_zscore']=(df['QNetprofitgrowth_zscore']+df['QRevenuegrowth_zscore'])/2
        df['Market_zscore']=df['turnoverweek_zscore']
        df['Value_zscore']=(df['PE_zscore']+df['PB_zscore']+df['PS_zscore'])/3
        #df[['Quality_zscore','Growth_zscore','Market_zscore','Value_zscore']]=df[['Quality_zscore','Growth_zscore','Market_zscore','Value_zscore']].apply(lambda x: x.fillna(0))
        for fac in (['Quality','Growth','Market','Value']):
            df[fac+'_zscore']=stats.zscore(df[fac+'_zscore'],nan_policy='omit')
            df[fac+'_Q']=pd.qcut(df[fac+'_zscore'],5,labels=[1,2,3,4,5],duplicates='drop')
        return(df)
    
    def A_ShsPrez(self,startdate,dailyreturn):
        today=date.today()
        todayname=str(today.strftime("%Y-%m-%d"))
        dfM=self.A_Merge(startdate,dailyreturn)
        dfM['Mcap']=dfM['Mcap'].astype(float)
        lowmcapmark=np.percentile(dfM['Mcap'],30)
        dfTop70=dfM.loc[dfM['Mcap']>=lowmcapmark,:].copy()
        rechist=self.DC.Hotstock(startdate)
        universe=self.A_Analysis(dfM)
        fulluniverse=self.A_Analysis(dfTop70)
        universe=pd.merge(universe,rechist[['ticker','RecomCounts']],on='ticker',how='left')
        fulluniverse['ticker']=[str(x)+' CH'for x in fulluniverse['ticker']]
        universe['ticker']=[str(x)+' CH'for x in universe['ticker']]
        fulluniverse=pd.merge(fulluniverse,rechist[['ticker','RecomCounts']],on='ticker',how='left')
        fulluniverse.to_csv("D:/CompanyData/GentableFullUni_"+todayname+".csv",index=False)
        universe.to_csv("D:/CompanyData/Gentable_"+todayname+".csv",index=False)
        return(universe,fulluniverse)