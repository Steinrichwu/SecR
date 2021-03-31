# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 10:15:11 2020

@author: wudi
"""


import pandas as pd
import numpy as np
from Toolbox import DataCollect
from Toolbox import WeightScheme
from MSSQL import MSSQL
from Toolbox import ReturnCal
from Toolbox import DataStructuring 
from Querybase import Query
from scipy import stats
from Quant import Optimize
from HotStock import SecR
from Consensus import Prep as ConsensusP


DC=DataCollect()
RC=ReturnCal()
DS=DataStructuring()
Q=Query()
WS=WeightScheme()
Opt=Optimize()
SR=SecR()
CP=ConsensusP()
#rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
#dailyreturn=DC.Dailyreturn_retrieve()

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
        df=df.loc[df['publdate'].isin(rebaldaylist),:].copy()
        return(df)

#Extract the rebaldays turnover on each stocks    
    def Turnover_extract(self,rebaldaylist,dailyreturn,signal):
        sigtab=dailyreturn.loc[dailyreturn['date'].isin(rebaldaylist),['date','ticker',signal]].copy()
        sigtab[signal]=1/sigtab[signal]                                          #The reciprocal of turnover is positively correlated with P&L
        sigtab['publdate']=sigtab['date']
        sigtab=sigtab.rename(columns={'date':'enddate'})
        sigtab=sigtab.rename(columns={signal:'sigvalue'})
        sigtab=sigtab[['publdate','enddate','ticker','sigvalue']]
        return(sigtab)

#Prepare the data: Download the underlying signal data, and stack them into one dataframe
#siglist=['PE','ROETTM','Valuecon_pe']
    def SigdataPrep(self,dailyreturn,siglist,rebaldaylist):
        startdate=rebaldaylist[0]
        sighist=pd.DataFrame(columns=['publdate','enddate','ticker','sigvalue','signame'])
        for signal in siglist:
            print('downloading:'+signal)
            if signal in (['PE','PB','PCF','PS']):
                sigtab=self.ValuationReciprocal_download(rebaldaylist,signal)
            elif signal in (['turnoverweek']):                                           #Market relevant signals, from the dailyreturn file
                sigtab=self.Turnover_extract(rebaldaylist,dailyreturn,signal)
            elif 'con_' in signal:
                sigtab=CP.Consensus(signal,rebaldaylist)
            else:
                sigtab=self.Funda_download(startdate,signal)
            sigtab['signame']=signal
            sigtab['ticker']=sigtab['ticker'].astype(str)
            sighist=sighist.append(sigtab,ignore_index=True,sort=False)
        sighist=sighist.loc[sighist['sigvalue'].isnull()==False,:]
        sighist=sighist.sort_values(by=['ticker','publdate'],ascending=[True,True])
        sighist=sighist.loc[sighist['ticker'].str[0].isin(['6','0','3'])].copy()           #Ashs only 6:shanghai,0:shenzhen,3:Chinext
        sighist['publdate']=pd.to_datetime(sighist['publdate'])
        sighist['enddate']=pd.to_datetime(sighist['enddate'])
        sighist=sighist.loc[sighist['sigvalue'].isnull()==False,:].copy()
        sighist=sighist.sort_values(by=['ticker','publdate'],ascending=[True,True])
        return(sighist)
    
    #Put all 5Q's pnl into one dataframe, each signal has its own datafarme of daily PNL        
    def PNLC(self,dailyreturn,portdict):
        #portdict={k:pd.DataFrame(v,columns=['ticker','mcap','Q','date','PortNav%'])for k,v in portdict.items()} #APply functiton to lists
        portPNL={k:RC.DailyPNL(dailyreturn,v)for k,v in portdict.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        portCumPNL={k:np.exp(np.log1p(v['dailyreturn']).cumsum())for k,v in portPNL.items()}
        PNLdict={}
        itemlist=portdict.keys()
        facnamelist=[x.split('_')[0]for x in itemlist]
        numlist=[x.split('_')[1]for x in itemlist]
        numlist=[int(x) for x in numlist]
        numlist.sort()
        facnamelist=list(set(facnamelist))
        for facname in facnamelist:
            tempdict={}
            for qn in numlist:
                tempdict[qn]=portCumPNL[facname+'_'+str(qn)]
            PNLdict[facname]=pd.DataFrame.from_dict(tempdict)
            PNLdict[facname].insert(0,column='date',value=portPNL[facname+'_'+str(numlist[0])]['date'])            
        return(PNLdict)
    
#Prepare the univese. universecode='Shen56'/'Market'/'04'(CSI)/'61'(CITIC)
#Return the stock universe of each rebalday, carrying information of mcap and sector's primecode
    def Universe(self,dailyreturn,rebaldaylist,universecode):
        activeuniverse=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist))&(dailyreturn['dailyvolume']!=0),['date','ticker','mcap']].copy() #Active
        if universecode!='Shen56':
            activeuniverse=DC.Keep_only_70mcapNew(activeuniverse)          #>30%mcap of the day
        activeuniverse['index']=activeuniverse['date']+activeuniverse['ticker']
        if universecode=='Market':
            universe=activeuniverse.copy()
        elif universecode=='Shen56':
            universe=pd.read_csv("D:/SecR/Shen56New.csv")
            universe['ticker']=[str(x).zfill(6)for x in universe['ticker']]
            universe=DS.Rebalday_alignment(universe,rebaldaylist)
        elif universecode=='Shen56latest':
            universe1=pd.read_csv("D:/SecR/ThreeFourlatest.csv")
            universe1=[str(x).zfill(6)for x in universe1['ticker']]
            universe2=pd.read_csv("D:/SecR/DailyShen6.csv")
            universe2['ticker']=[str(x).zfill(6)for x in universe2['ticker']]
            universe1.extend(universe2['ticker'])
            universe=pd.DataFrame(list(set(universe1)),columns=['ticker'])
            universe['date']=rebaldaylist[0]
        elif universecode[0]=='0':
            universe=DC.Ashs_stock_seccode(rebaldaylist,[universecode],'CSI')             #CSI sector
        elif int(universecode[0])>0:
            universe=DC.Ashs_stock_seccode(rebaldaylist,[universecode],'CITIC')           #CITIC sector
        universe=universe[['date','ticker']]
        stock_sector=DC.Stock_sector(rebaldaylist,list(universe['ticker'].unique()),'CITIC')
        stock_sector['index']=stock_sector['date']+stock_sector['ticker']
        universe['index']=universe['date']+universe['ticker']
        universe=pd.merge(universe,stock_sector[['index','primecode']],on='index',how='left')
        universe=pd.merge(universe,activeuniverse[['index','mcap']],on='index',how='left')
        universe=universe.loc[universe['mcap'].isnull()==False,['date','ticker','mcap','primecode']].copy()
        return(universe) 
    
    #universecode='Market'/'Shen56'/'00'/'Shen5600' (But it isn't useful to use Shen5600, as Shen56 on each sector has only a few stocks)
    def UniverseNEW(self,dailyreturn,rebaldaylist,universecode):
        activeuniverse=dailyreturn.loc[(dailyreturn['date'].isin(rebaldaylist))&(dailyreturn['dailyvolume']!=0),['date','ticker','mcap']].copy() #Active
        activeuniverse['index']=activeuniverse['date']+activeuniverse['ticker']
        if universecode[0:6]=='Shen56':
            baseuniverse=pd.read_csv("D:/SecR/Shen56New.csv")
            baseuniverse['ticker']=[str(x).zfill(6)for x in baseuniverse['ticker']]
            baseuniverse=DS.Rebalday_alignment(baseuniverse,rebaldaylist)
            baseuniverse['index']=baseuniverse['date']+baseuniverse['ticker']
            baseuniverse=baseuniverse.loc[baseuniverse['index'].isin(activeuniverse['index']),:].copy()
        else:
            baseuniverse=DC.Keep_only_70mcapNew(activeuniverse)
        newuniversecode=universecode.replace('Shen56','')
        if len(newuniversecode)==2:
            if newuniversecode[0]=='0':
                secuniverse=DC.Ashs_stock_seccode(rebaldaylist,[newuniversecode],'CSI')             #CSI sector
                secuniverse['index']=secuniverse['date']+secuniverse['ticker']
                baseuniverse=secuniverse.loc[secuniverse['index'].isin(baseuniverse['index']),:].copy()
            elif int(newuniversecode[0])>0:
                secuniverse=DC.Ashs_stock_seccode(rebaldaylist,[newuniversecode],'CITIC')           #CITIC sector
                secuniverse['index']=secuniverse['date']+secuniverse['ticker']
                baseuniverse=secuniverse.loc[secuniverse['index'].isin(baseuniverse['index']),:].copy()
        baseuniverse=baseuniverse.drop(['index'],axis=1)
        return(baseuniverse)
    
class BTStruct():
    def __init__(self):
        self.P=Prep()
    
    #Construct a dataframe to be used for neutrliaztion (single signal single rebalday)
    def Current_one_signal(self,current_onesig):
        current_onesigWS=DS.Winsorize(current_onesig,'sigvalue',0.015)                                         #Winsorize a given column
        indu_dummy=pd.get_dummies(current_onesigWS['primecode'])
        current_onesigWSMcapDummy=pd.concat([current_onesigWS,indu_dummy],axis=1)                  #Add dummy table
        current_onesigWSMcapDummy=current_onesigWSMcapDummy.replace(np.inf,np.nan)
        current_onesigWSMcapDummy=current_onesigWSMcapDummy.dropna()                                      #Dropna and np.inf
        current_onesigWSMcapDummy['sigvalue']=current_onesigWSMcapDummy['sigvalue'].astype(float)         
        Xset=list(indu_dummy.columns.insert(0,'mcap'))
        return(current_onesigWSMcapDummy,Xset)
    
    #Input: signalhist,sechist. Loop through every rebalday,on every rebalday, loop through Selectsigs,neutralize every signal in Selectsigs
    #Return: A dictionary of every signal's neutralized value and its Zscore on every rebalday
    def NSighist(self,dailyreturn,rebaldaylist,sighist,selectsigs,universe):
        sighist['index']=sighist['ticker']+sighist['signame']
        nsigdict={}
        for rebalday in rebaldaylist:
            print(rebalday)
            rebaldatetime=pd.to_datetime(rebalday)
            sighist['updatelag']=(sighist['publdate']-rebaldatetime).dt.days    
            rebal_stocklist=list(universe.loc[universe['date']==rebalday,'ticker'].unique())
            rebal_universe=universe.loc[universe['date']==rebalday,:].copy()
            rebal_sighist=sighist.loc[(sighist['publdate']<=rebalday)&(sighist['ticker'].isin(rebal_stocklist))&(sighist['updatelag']>=-180),:].copy()
            rebal_sighist=rebal_sighist.drop_duplicates('index','last')
            rebal_sighist=pd.merge(rebal_sighist,rebal_universe[['ticker','mcap','primecode']],on='ticker',how='left')
            rebal_sighist['mcap']=rebal_sighist['mcap'].apply(np.log)
            for sig in selectsigs:
                current_onesig=rebal_sighist.loc[rebal_sighist['signame']==sig,:].copy()
                current_onesigWSMcapDummy,Xset=self.Current_one_signal(current_onesig) #Prepare the table with sector dummy and marketcap, to be neutrliazed
                sig_rebalday=DS.Neutralization(current_onesigWSMcapDummy,sig,Xset)                                          #Neutralized sig values
                sig_rebalday=sig_rebalday.rename(columns={'sigvalue':sig})
                sig_rebalday=DS.Winsorize(sig_rebalday,'N_'+sig,0.05) 
                sig_rebalday[sig+'_zscore']=stats.zscore(sig_rebalday['N_'+sig])
                nsigdict[sig+'_'+rebalday]=sig_rebalday
        return(nsigdict)
    
       
    #Factor consists of different signal. given Quality's siginfac=['ROETTM','ROATTM'],facname='Quality' 
    #Input: factorname and the signals under the factor. Output the synthesized Zscore of each stock on one Factor
    def Factorscore(self,rebaldaylist,nsigdict,facname,siginfac,factorZ):
        siginfacz=[sig+'_zscore' for sig in siginfac]
        for rebalday in rebaldaylist:
            zscoretab=pd.DataFrame(nsigdict[siginfac[0]+'_'+rebalday]['ticker'],columns=['ticker'])
            for sig in siginfac:
                zscoretab=pd.merge(zscoretab,nsigdict[sig+'_'+rebalday][['ticker',sig,'N_'+sig,sig+'_zscore']],on='ticker',how='outer')
            zscoretab[facname+'_zscore']=np.nanmean(zscoretab[siginfacz],axis=1)
            zscoretab=zscoretab.replace([np.inf, -np.inf], np.nan)
            zscoretab=zscoretab.dropna()
            if len(siginfacz)>1:
                zscoretab[facname+'_zscore']=stats.zscore(zscoretab[facname+'_zscore'])
            zscoretab['Q']=pd.qcut(zscoretab[facname+'_zscore'],5,labels=[1,2,3,4,5],duplicates='drop')  #return the grouping of the signame column of a dataframe
            factorZ[facname+'_'+rebalday]=zscoretab
        return(factorZ)
    
    #Produce the Fzdict (each factor's zscore on every stock on ONE rebalday: 'Quality_2019-05-06')
    def Fzdict(self,dailyreturn,rebaldaylist,facdict,universecode):
        selectsigs=[]
        [selectsigs.extend(v) for k, v in facdict.items()]
        siglist=list(set([x.replace('growth','') for x in selectsigs]))
        siglist=list(set([x.replace('vol','') for x in siglist]))
        facnamelist=list(facdict.keys())
        sighist=self.P.SigdataPrep(dailyreturn,siglist,rebaldaylist)                                #All fundadata of basic signals
        sighist=DS.GrowVol(sighist,'grow') 
        universe=self.P.Universe(dailyreturn,rebaldaylist,universecode)                                                         #All growthdata of basic signals
        nsigdict=self.NSighist(dailyreturn,rebaldaylist,sighist,selectsigs,universe)                       #Neutralize selected signals over rebaldaylist
        fzdict={}
        for facname in facnamelist:
            siginfac=facdict[facname]
            fzdict=self.Factorscore(rebaldaylist,nsigdict,facname,siginfac,fzdict)                #calculate added zscore of factor and Group it into 5Q
        return(fzdict)
    
    #Merge each stock's factors' zscore(not signals') together in time series, all in one table 
    def FZtab(self,fzdict):
        itemlist=fzdict.keys()
        facnamelist=list(set([x.split('_')[0]for x in itemlist]))
        rebaldaylist=list(set([x.split('_')[1]for x in itemlist]))
        rebaldaylist.sort()
        colnames=['date','ticker']+[x+'_zscore'for x in facnamelist]
        fztab=pd.DataFrame(columns=colnames)
        for rebalday in rebaldaylist:
            rebalztab=pd.DataFrame()
            for facname in facnamelist:
                ztab=fzdict[facname+'_'+rebalday][['ticker',facname+'_zscore']].copy()
                ztab=ztab.drop_duplicates()
                if rebalztab.shape[0]==0:
                   rebalztab=ztab.copy()
                else:
                   rebalztab=pd.merge(rebalztab,ztab,on='ticker',how='outer')
            rebalztab.insert(0,column='date',value=rebalday)            
            fztab=fztab.append(rebalztab)
        #fztab=fztab.drop('index', 1)
        return(fztab)
 
    #convert the qdict into portfolio holdings 
    def Portdict(self,olddict,rebaldaylist):
        itemlist=olddict.keys()
        facnamelist=[x.split('_')[0]for x in itemlist]
        facnamelist=list(set(facnamelist))
        portdict={}
        for fac in facnamelist:
            portdict=DS.Facqport(olddict,fac,rebaldaylist,portdict)
        portdict={k:pd.DataFrame(v,columns=['ticker','date','PortNav%'])for k,v in portdict.items()}
        return(portdict)

class Review():
    def __init__(self):
        self.P=Prep()
        self.F=BTStruct()
      #The process of Backtest on Factor level (each factor consists of different signals)
      #THreeFour with no sector factor =>Q5 19x return; only 13ish if including sector factor
      #Shen6 inclding sector factor outperformed ThreeFour with factor and on par with ThreeFour WITHOUT factor, since 2010 tho
      #CSIsecnum='04',ranktype='5Q'/'3Q'/'Top50'/Top%10; universetype='Sector'
      #CSIsecnum='N', universetype='ThreeFour'/'Sector',startdate='2006-12-28',CSIsecnum='N',facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek']}
    def IntegratedBT(self,dailyreturn,rebaldaylist,facdict,ranktype,universecode):
        facinfacz=[x+'_zscore' for x in facdict.keys()]
        fzdict=self.F.Fzdict(dailyreturn,rebaldaylist,facdict,universecode)                                                             
        fztab=self.F.FZtab(fzdict)
        fztab['meanscore']=np.nanmean(fztab[facinfacz],axis=1) #included those dont have score in a factor
        olddict={}
        for rebalday in rebaldaylist:
            rebalz=fztab.loc[fztab['date']==rebalday,:].copy()
            rebalz['rank']=rebalz['meanscore'].rank(method='first')
            if ranktype=='5Q':
                rebalz['Q']=pd.qcut(rebalz['rank'].values,5,labels=[1,2,3,4,5])
                olddict['meanscore_'+rebalday]=rebalz
            elif ranktype=='3Q':
                rebalz['Q']=pd.qcut(rebalz['rank'].values,3,labels=[1,2,3])
                olddict['meanscore_'+rebalday]=rebalz
            elif ranktype[0:4]=='Top%':
                npct=int(ranktype[4:])
                n=int(np.ceil(rebalz.shape[0]*npct/100))
                #print('n=',n)
                rebalz=rebalz.nlargest(n,'rank',keep='all')
                olddict['meanscore_'+rebalday]=rebalz
            elif ranktype[0:3]=='Top':
                n=int(ranktype[3:])
                rebalz=rebalz.nlargest(n,'rank',keep='all')
                olddict['meanscore_'+rebalday]=rebalz
        portdict=self.F.Portdict(olddict,rebaldaylist)                                               ##calculate added zscore of factor and Group it into 5Q,Generate each Factor's holdings
        #print('calculatingPNL')
        #PNLcumdict=self.P.PNLC(dailyreturn,portdict)                                                #Calculate each factors' 5 PNL lines
        return(portdict,fzdict,fztab)
   
    