# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 20:42:17 2020

@author: wudi
"""
import pandas as pd
import numpy as np
from Toolbox import DataCollect
from MSSQL import MSSQL
from Toolbox import ReturnCal
from Toolbox import DataStructuring 
from Querybase import Query

DC=DataCollect()
RC=ReturnCal()
DS=DataStructuring()
Q=Query()



class Prep():
    def __init__(self):
        pass
    
    #use the query in Querybase to download hitorical signal
    def Funda_download(self,startdate,signal):
        ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
        query=getattr(Q,signal)(startdate)
        reslist=ms.ExecQuery(query)
        df=pd.DataFrame(reslist,columns=['publdate','enddate','ticker','sigvalue'])
        return(df)

#use the query in Querybase to download hitorical for valuation related signals only
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

#Prepare the data: Download the underlying signal data, and stack them into one dataframe
    def SigdataPrep(self,dailyreturn,startdate,rebal_period,siglist,rebaldaylist):
        sighist=pd.DataFrame(columns=['publdate','enddate','ticker','sigvalue','signame'])
        for signal in siglist:
            print('downloading:'+signal)
            if signal in (['PE','PB','PCF','PS']):
                sigtab=self.ValuationReciprocal_download(rebaldaylist,signal)
                sigtab=sigtab.loc[sigtab['publdate'].isin(rebaldaylist),:]
            elif signal in (['turnoverweek']):                                           #Market relevant signals, from the dailyreturn file
                sigtab=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),['date','ticker',signal]].copy()
                sigtab[signal]=1/sigtab[signal]
                sigtab['publdate']=sigtab['date']
                sigtab=sigtab.rename(columns={'date':'enddate'})
                sigtab=sigtab.rename(columns={signal:'sigvalue'})
                sigtab=sigtab[['publdate','enddate','ticker','sigvalue']]
            else:
                sigtab=self.Funda_download(startdate,signal)
            sigtab['signame']=signal
            sigtab['ticker']=sigtab['ticker'].astype(str)
            sighist=sighist.append(sigtab,ignore_index=True,sort=False)
        sighist=sighist.loc[sighist['sigvalue'].isnull()==False,:]
        sighist=sighist.sort_values(by=['ticker','publdate'],ascending=[True,True])
        sighist['publdate']=pd.to_datetime(sighist['publdate'])
        return(sighist)

#Put all 5Q's pnl into one dataframe, each signal has its own datafarme of daily PNL        
    def PNLC(self,dailyreturn,portdict,siglist):
        portdict={k:pd.DataFrame(v,columns=['ticker','mcap','Q','date','PortNav%'])for k,v in portdict.items()}
        portPNL={k:RC.DailyPNL(dailyreturn,v)for k,v in portdict.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        PNLdict={}
        for signame in siglist:
            tempdict={}
            for qn in list(range(1,6)):
                tempdict[qn]=portCumPNL['Q_'+signame+'_'+str(qn)]
            PNLdict[signame]=pd.DataFrame.from_dict(tempdict)
            PNLdict[signame].insert(0,column='date',value=portPNL['Q_'+signame+'_1']['date'])            
        return(PNLdict)

class Funda():
    def __init__(self):
        self.P=Prep()
    
    #Construct a dataframe to be used for neutrliaztion (single signal single rebalday)
    def Current_one_signal(self,sig,current_sig,rebalday,Mcap_rebalday,stock_sector):
        current_onesig=current_sig.loc[(current_sig['publdate']<=rebalday)&(current_sig['signame']==sig),['publdate','ticker','sigvalue','signame']]
        current_onesig=current_onesig.sort_values(by=['publdate','ticker'],ascending=[True,True])    
        current_onesig=current_onesig.drop_duplicates(['ticker'],keep="last")                            #Take entries that are closest to rebalday of each stock
        #current_onesigWS=DS.Winsorize(current_onesig,'sigvalue')                                         #Winsorize a given column
        current_onesigWSMcap=pd.merge(current_onesig,Mcap_rebalday[['ticker','mcap']],on='ticker',how='left')  #Add Mcap
        current_onesigWSMcapSec=pd.merge(current_onesigWSMcap,stock_sector.loc[stock_sector['date']==rebalday,['ticker','primecode']],on='ticker',how='left') #Add Sector
        current_onesigWSMcapSec=current_onesigWSMcapSec[~current_onesigWSMcapSec['primecode'].isin(['40','41'])] #NONFIG stocks only
        indu_dummy=pd.get_dummies(current_onesigWSMcapSec['primecode'])
        current_onesigWSMcapDummy=pd.concat([current_onesigWSMcapSec,indu_dummy],axis=1)                  #Add dummy table
        current_onesigWSMcapDummy=current_onesigWSMcapDummy.replace(np.inf,np.nan)
        current_onesigWSMcapDummy=current_onesigWSMcapDummy.dropna()                                      #Dropna and np.inf
        current_onesigWSMcapDummy['sigvalue']=current_onesigWSMcapDummy['sigvalue'].astype(float)         
        current_onesigWSMcapDummy['mcap']=current_onesigWSMcapDummy['mcap'].astype(float)
        Xset=list(indu_dummy.columns.insert(0,'mcap'))
        return(current_onesigWSMcapDummy,Xset)
    
    #Input: signalhist,sechist. Loop through every rebalday,on every rebalday, loop through signals,neutralize every signal 
    def Porthist(self,dailyreturn,rebaldaylist,sighist,selectsigs):
        sighist=sighist.loc[sighist['sigvalue'].isnull()==False,:]
        tickerlist=sighist['ticker'].unique().tolist()
        stock_sector=DC.Stock_sector(rebaldaylist,tickerlist)
        portdict={}
        Rdailyreturn=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),:].copy()
        Rdailyreturn['mcap']=Rdailyreturn['mcap'].apply(np.log)
        for rebalday in rebaldaylist:
            print(rebalday)
            activestocks_rebalday=dailyreturn.loc[(dailyreturn['date']==rebalday)&(dailyreturn['dailyreturn']!=0),:]['ticker'].unique().tolist() #stocks that are tradign on rebalday
            rebaldatetime=pd.to_datetime(rebalday)
            sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days                                   #current sig is the snapshot of siglist in the last ENDdate prior to rebal day
            current_sig=sighist.loc[(sighist['updatelag']>=-180)&(sighist['updatelag']<=0),:]                  #remove the entries where publish dates is more than half a year ago
            current_sig=current_sig.loc[current_sig['ticker'].isin(activestocks_rebalday),:]                   #only stocks tradign on rebalday participate in the analysis
            Mcap_rebalday=Rdailyreturn.loc[(Rdailyreturn['date']==rebalday)&(Rdailyreturn['ticker'].isin(current_sig['ticker'].unique())),:].copy()
            for sig in selectsigs:
                current_onesigWSMcapDummy,Xset=self.Current_one_signal(sig,current_sig,rebalday,Mcap_rebalday,stock_sector) #Prepare the table with sector dummy and marketcap, to be neutrliazed
                sig_rebalday=DS.Neutralization(current_onesigWSMcapDummy,sig,Xset)                                          #Neutralized sig values
                Q_sig_rebalday=DS.Qgrouping(sig,sig_rebalday,5)                                                             #QGroup of neturliazed value
                #portdict=DS.Qport(Q_sig_rebalday,'Q_'+sig,rebalday,portdict)
                portdict=DS.Qport2(Q_sig_rebalday,'Q_'+sig,rebalday,portdict)
        return(portdict)    
 
    #dailyreturn=DC.Dailyreturn_retrieve()
    #startdate='2006-12-28'
    #rebal_period=20
    #siglist=['PE','ROECut','Revenue_YOY']
    def Fundabacktest(self,dailyreturn,startdate,rebal_period,siglist):
        rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
        #fundasiglist=[str.replace(x,"_growthrate","") for x in siglist]
        sighist=self.P.SigdataPrep(dailyreturn,startdate,rebal_period,siglist,rebaldaylist)
        #growthsighist=DS.GrowthFunda(sighist)
        portdict=self.Porthist(dailyreturn,rebaldaylist,sighist,siglist)
        print('calculating return')
        PNLcumdict=self.P.PNLC(dailyreturn,portdict,siglist)
        return(PNLcumdict)
        
    
 

