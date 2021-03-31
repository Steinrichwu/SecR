# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 11:30:49 2019

@author: wudi
"""
import pandas as pd

Existing=pd.read_csv("D:/SecR/Rebal/Existing.csv")
ToOwn=pd.read_csv("D:/SecR/Rebal/Target.csv")

ToOwn['ToOwn']=ToOwn['ToOwn'].astype(float)
Rebal=pd.merge(ToOwn,Existing,on='Ticker',how='outer')
Rebal=Rebal.fillna(0)
Rebal['ToTrade']=Rebal['ToOwn']-Rebal['Existing']
Rebal.loc[Rebal['ToTrade']<0,'Side']='Sell'
Rebal.loc[Rebal['ToTrade']>0,'Side']='Buy'
Rebal['Quantity']=Rebal['ToTrade'].abs()
Rebal=Rebal.loc[Rebal['Quantity']!=0,:]
Rebal.to_csv("D:/SecR/Rebal/Trade.csv",index=False)
