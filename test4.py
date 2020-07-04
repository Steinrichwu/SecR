# -*- coding: utf-8 -*-
"""
Created on Sat Jun  6 17:22:34 2020

@author: wudi
"""

import scipy.optimize as sco
import numpy as np
import numpy.random as npr 
import pandas as pd
number=1000

log_returns=pd.read_csv("D:/MISC/returntemp.csv")

stock_num=len(log_returns.columns)
weights=npr.rand(number,stock_num)
weights/=np.sum(weights,axis=1).reshape(number,1)
prets=np.dot(weights,log_returns.mean())
pvols=np.diag(np.sqrt(np.dot(weights,np.dot(log_returns.cov()*252,weights.T))))

def statistics(weights):
    weights=np.array(weights)
    pret=np.sum(log_returns.mean()*weights.T*252)
    pvols=np.sqrt(np.dot(weights.T,np.dot(log_returns.cov(),252,weights)))
    return np.array([pret,pvols,pret/pvols])

def min_sharpe(weights):
    return -statistics(weights)[2]

cons=({'type':'eq','fun': lambda x: np.sum(x)-1})
bnds=tuple((0,1)for x in range(stock_num))
opts=sco.minimize(min_sharpe,stock_num*[1/stock_num],method='SLSQP',bounds=bnds,constraints=cons)
opts['x'].round(3)