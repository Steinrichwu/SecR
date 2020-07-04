# -*- coding: utf-8 -*-
"""
Created on Thu May 28 11:22:16 2020

@author: wudi
"""
import pandas as pd
import numpy as np
from MSSQL import MSSQL
from Toolbox import DataCollect
from Toolbox import ReturnCal
from Toolbox import WeightScheme

DC=DataCollect()
RC=ReturnCal()
dailyreturn=DC.Dailyreturn_retrieve()
WS=WeightScheme()

class Prep():
    def __init__(self):
        pass
    
    #use the query in Querybase to download hitorical signal
    def Bank_download(self):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        siglist=['23000','22800','55000','26200','26000','30240','21300']
        #indicatorname=['不良贷款比率','拨备覆盖率','核心一级资本充足','净息差','净利差','杠杆率','拨贷比'] #Banks ratios' code in 聚源Bankingn sheet
        query="Select FS.InfoPublDate,FS.EndDate,SM.SecuCode,FS.RatioEOP,FS.IndicatorCode from JYDBBAK.dbo.LC_FSpecialIndicators FS left join JYDBBAK.dbo.SecuMain SM on FS.CompanyCode=SM.CompanyCode where FS.Mark=2 and FS.Indicatorcode in ("+str(siglist)[1:-1]+") and  SM.SecuCategory = 1 and FS.InfoPublDate>'2003-01-01'"
        reslist=ms.ExecQuery(query)
        df=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','sigvalue','signame'])
        sighist=df.loc[df['ticker'].str[0].isin(['6','0'])].copy()
        sighist['sigvalue']=sighist['sigvalue'].astype(float)
        sighist['signame']=sighist['signame'].astype(str)
        sighist=self.Ratio_treat(sighist)
        return(sighist)
    
    #Convert the negative ratio's order like不良贷款比率    
    def Ratio_treat(self,sighist):
        sighist.loc[sighist['signame']=='23000','sigvalue']=1/sighist.loc[sighist['signame']=='23000','sigvalue'] #23000:不良贷款比率
        return(sighist)
        
    #Put all 5Q's pnl into one dataframe, each signal has its own datafarme of daily PNL        
    def PNLC(self,dailyreturn,portdict,siglist):
        #portdict={k:pd.DataFrame(v,columns=['ticker','mcap','Q','date','PortNav%'])for k,v in portdict.items()} #APply functiton to lists
        portPNL={k:RC.DailyPNL(dailyreturn,v)for k,v in portdict.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        PNLdict={}
        for signame in siglist:
            tempdict={}
            for qn in list(range(1,4)):
                tempdict[qn]=portCumPNL['Q_'+signame+'_'+str(qn)]
            PNLdict[signame]=pd.DataFrame.from_dict(tempdict)
            PNLdict[signame].insert(0,column='date',value=portPNL['Q_'+signame+'_1']['date'])            
        return(PNLdict)

class Funda():
    def __init__(self):
        self.P=Prep()
        
    #Everyrebalday's Q-grouping of selected signals:
    def Currnet_signal(self,sighist,rebalday,selectsigs,portdict):
        activestocks_rebalday=dailyreturn.loc[(dailyreturn['date']==rebalday)&(dailyreturn['dailyreturn']!=0),:]['ticker'].unique().tolist() #stocks that are tradign on rebalday
        rebaldatetime=pd.to_datetime(rebalday)
        sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days     
        current_sig=sighist.loc[(sighist['updatelag']>=-180)&(sighist['updatelag']<=0),:]    
        current_sig=current_sig.loc[current_sig['ticker'].isin(activestocks_rebalday),:]
        for sig in selectsigs:
            current_onesig=current_sig.loc[(current_sig['publdate']<=rebalday)&(current_sig['signame']==sig),['publdate','ticker','sigvalue','signame']]
            current_onesig=current_onesig.sort_values(by=['publdate','ticker'],ascending=[True,True])    
            current_onesig=current_onesig.drop_duplicates(['ticker'],keep="last")             
            qlabels=[1,2,3]
            qgroup=pd.qcut(current_onesig['sigvalue'],3,labels=qlabels)
            colname='Q_'+str(sig)
            current_onesig[colname]=qgroup
            for q in qlabels:
                newqport=current_onesig.loc[current_onesig[colname]==q,['ticker',colname]]
                newqport['date']=rebalday
                newqport['PortNav%']=1/newqport.shape[0]
                portindex=colname+'_'+str(q)
                newqportlist=newqport.values.tolist()
                if not portindex in portdict:
                    portdict[portindex]=[]
                portdict[portindex].extend(newqportlist)
        return(portdict)
        
    #Generate the historical signal+Q's model portfolio: EqualWeight
    def Porthist(self,dailyreturn,rebaldaylist,sighist,selectsigs):
        portdict={}
        for rebalday in rebaldaylist:
            portdict=self.Currnet_signal(sighist,rebalday,selectsigs,portdict)
        portdict={k:pd.DataFrame(v,columns=['ticker','Q','date','PortNav%'])for k,v in portdict.items()}
        return(portdict)
    
    #backtest
    #startdate='2014-06-30'
    #rebal_period=20
    #selectsigs=['23000','22800','55000','26200','26000','30240','21300']
    
    def Fundabacktest(self,dailyreturn,startdate,rebal_period,selectsigs):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        #fundasiglist=[str.replace(x,"_growthrate","") for x in siglist]
        sighist=self.P.Bank_download()
        #growthsighist=DS.GrowthFunda(sighist)
        portdict=self.Porthist(dailyreturn,rebaldaylist,sighist,selectsigs)
        print('calculating return')
        PNLcumdict=self.P.PNLC(dailyreturn,portdict,selectsigs)
        return(PNLcumdict,portdict)
    
    #Choose stocks that are most frequently selected in group3 
    def Pick_top_stocks(self,dailyreturn,startdate,rebal_period,selectsigs):  
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        sighist=self.P.Bank_download()
        portdict=self.Porthist(dailyreturn,rebaldaylist,sighist,selectsigs)
        picked=pd.DataFrame(columns=['date','ticker'])
        picklist=['Q_'+sig+'_1' for sig in selectsigs]
        for key in picklist:
            picked=picked.append(portdict[key][['date','ticker']])
        picked['index']=picked['date']+picked['ticker']
        valuecounts=pd.DataFrame(picked['index'].value_counts())                          #how many times selected (once per signal)
        valuecounts.reset_index(inplace=True)
        valuecounts.columns=['index','counts']
        picked_shortlisted=pd.merge(picked,valuecounts,on='index',how='left')
        picked_shortlisted=picked_shortlisted.loc[picked_shortlisted['counts']>=4,:]                     #Select those that make 3 points, when the totalpoints=number of sigs, 就是交集
        picked_shortlisted=picked_shortlisted.drop_duplicates(['index'],keep="first")
        picked_shortlisted=picked_shortlisted.drop('index', 1)
        #picked_shortlisted=WS.Benchmark_intersect(picked_shortlisted,'CSI300')    #Intersection with CSI300
        picked_shortlisted['weight']=1                                          #等权
        postab=WS.Generate_PortNav(picked_shortlisted)
        SPNL=RC.DailyPNL(dailyreturn,postab)                                     
        CumPNL=RC.CumPNL(SPNL)
        return(CumPNL,postab)
    
    