# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 12:41:08 2021

@author: wudi
"""
import numpy as np
import pandas as pd
import random
import math
import matplotlib.pylab as plt

def RandomGenerate():
    base=0
    for i in range(1,13):
        ran=random.random()
        base=base+ran
    rand=base-6
    return(rand)

#vol=0.2
#drift=0.1
def Randomwalk(vol,drift):
    timestep=1/250
    pricelist=[100]
    returnlist=[]
    for i in range(0,2000):
        rand=RandomGenerate()
        dailyreturn=(1+drift*timestep+(vol)*math.sqrt(timestep)*rand)
        newprice=pricelist[i]*dailyreturn
        pricelist.append(newprice)
        returnlist.append(dailyreturn)
    pricelist=pd.DataFrame(pricelist,columns=['close'])
    std=np.std(returnlist)
    #print(std*16)
    #plt.plot(pricelist['close'])
    return(pricelist)

def Simulation():
    returnlist=[]
    for i in range(0,10000):
        returnhist=Randomwalk(0.2,0.1)
        totalreturn=returnhist['close'][returnhist.shape[0]-1]
        returnlist.append(totalreturn)
    mean=np.mean(returnlist)
    print(mean)
    return()