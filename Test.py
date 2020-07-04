# -*- coding: utf-8 -*-
"""
Created on Sat May 30 09:50:01 2020

@author: wudi
"""
import numpy as np

def newgrowth(sighist):
    sighist['sigvalue']=sighist['sigvalue'].astype(float)
    sighist['enddate']=sighist['enddate'].astype(str)
    sighist['month']=sighist['enddate'].str[5:7].astype(int)
    sighist['year']=sighist['enddate'].str[0:4].astype(int)
    sighist=sighist.sort_values(by=['ticker','signame','month','enddate'],ascending=[True,True,True,True])
    sighist['monthdiff']=sighist['month']-sighist['month'].shift(1)
    sighist['yeardiff']=sighist['year']-sighist['year'].shift(1)
    sighist['tickerdiff']=sighist['ticker']==sighist['ticker'].shift(1)
    sighist['signamediff']=sighist['signame']==sighist['signame'].shift(1)
    #if growvol=='grow':
    sighist['growth']=(sighist['sigvalue']-sighist['sigvalue'].shift(1))/abs(sighist['sigvalue'].shift(1))
    #elif growvol=='vol':
    #    sighist['vol']=sighist['sigvalue'].rolling(5).std()
    sighist.loc[(sighist['monthdiff']!=0)|(sighist['yeardiff']!=1)|(sighist['tickerdiff']!=True)|(sighist['signamediff']!=True),'growth']=np.nan
    sighist=sighist[['publdate','enddate','ticker','sigvalue','signame','growth']]
    return(sighist)
    

def Growvol(sighist,growvol):
    sgv=sighist.copy()
    sgv['sigvalue']=sgv['sigvalue'].astype(float)
    sgv['enddate']=sgv['enddate'].astype(str)
    sgv['month']=sgv['enddate'].str[5:7].astype(int)
    sgv['year']=sgv['enddate'].str[0:4].astype(int)
    sgv=sgv.sort_values(by=['ticker','signame','month','enddate'],ascending=[True,True,True,True])
    sgv['yeardiff']=sgv['year']-sgv['year'].shift(1)
    sgv['index']=sgv['ticker']+sgv['signame']+sgv['month'].astype(str)
    sgv['nthoccur']=sgv.groupby('index').cumcount()+1
    if growvol=='grow':
        sgv['deriv']=(sgv['sigvalue']-sgv['sigvalue'].shift(1))/abs(sgv['sigvalue'].shift(1))
        sgv.loc[(sgv['nthoccur']==1)|(sgv['yeardiff']!=1),'growth']=np.nan
        sgv['signame']=sgv['signame']+'_growth'
    elif growvol=='vol':
        sgv['deriv']=sgv['sigvalue'].rolling(3).std()
        sgv.loc[(sgv['nthoccur']<3)|(sgv['yeardiff']!=1),'vol']=np.nan
        sgv['signame']=sgv['signame']+'_vol'
    sgv=sgv.loc[sgv['deriv']!=np.nan,:]
    sgv['sigvalue']=sgv['deriv']
    sgv=sgv[['publdate','enddate','ticker','sigvalue','signame']]
    sighist=sighist.append(sgv)
    return(sighist)