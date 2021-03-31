# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 13:21:55 2020

@author: wudi
"""
from Port import PortConst
from Port import Backtest
from Toolbox import DataCollect
from FundaStock import Review

PC=PortConst()
BT=Backtest()
DC=DataCollect()
FR=Review()
#dailyreturn=DC.Dailyreturn_retrieve()

#1. The 2020/09/02 Latest version of Shensuanzi ABC portfolio
def ShensuanziABC_20200902(dailyreturn):
    startdate='2009-12-28'
    rebal_period=10
    rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
    mergedPos=PC.PortBuild(dailyreturn,rebaldaylist)
    mergedPosSim=PC.SimpMerged(dailyreturn,mergedPos)
    mergedPNLSim=BT.GenPNL(mergedPosSim,dailyreturn)
    PNLreview=PC.Alpha(mergedPNLSim)
    return(PNLreview)

#2. The old version of Shen567 Old, produced in early July
def Shen567OLD_20200902(dailyreturn):
    startdate='2009-12-28'
    rebal_period=20
    facdict={'Quality': ['ROETTM', 'ROATTM'],'Growth': ['QRevenuegrowth', 'QNetprofitgrowth'],'Value': ['PE', 'PB', 'PS'],'Market':['turnoverweek'],'Sector':['SectorAlpha']}
    rebaldaylist=DC.Rebaldaylist(startdate,rebal_period)
    universetype='ThreeFour'
    CSIsecnum='N'
    ranktype='5Q'
    PNLcumdict,portdict,fzdict,fztab=FR.IntegratedBT(dailyreturn,rebaldaylist,facdict,universetype,ranktype,CSIsecnum)
    totalreturn=PNLcumdict['meanscore'][['date',5]].copy()
    totalreturn['dailyreturn']=totalreturn[5]/totalreturn[5].shift(1)
    totalreturn['dailyreturn']=totalreturn['dailyreturn']-1
    return(PNLcumdict)