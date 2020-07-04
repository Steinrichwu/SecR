# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 10:49:05 2020

@author: wudi
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize
from scipy.linalg import orth
from scipy.linalg import sqrtm, inv


class Optimize():
    def __init__(self):
        pass
    
    def Opt(self,port,bm):
        stock_num=port['weight'].shape[0]
        CSIcol=[x for x in bm.columns if x not in['date','ticker','weight']]
        CSIdummymatrix=bm[CSIcol].values
        Cweight=bm['weight']
        Cindustryexp=np.dot(Cweight,CSIdummymatrix)
        Adummymatirx=port[CSIcol].values
        def statistics(weights):
            weights=np.array(weights)
            t=port['TargetFactor']
            s=np.dot(weights.T,t)
            return (s)
        def fac_exposure_objective(weights):
            return(-statistics(weights))
        cons=({'type':'eq','fun': lambda x: np.sum(x)-1},
               {'type':'eq','fun': lambda x:-np.linalg.norm(np.dot(x,Adummymatirx)-Cindustryexp)})
        bnds=tuple((0.001,0.05) for x in range(stock_num))    
        res=minimize(fac_exposure_objective,[0]*stock_num,constraints=cons,bounds=bnds,method='SLSQP')
        return(res['x']) 

    def WLS_adjusted(self,othorebal,industry_weight,X):
        adj_weighted=np.sqrt(othorebal['mcap'])/np.sqrt(othorebal['mcap']).sum()                        #构建权重调整矩阵V
        V=pd.DataFrame(np.diag(adj_weighted),index=othorebal['ticker'],columns=othorebal['ticker'])     #构建权重调整矩阵V，V是一个diagonal matrix
        k=len(X.columns)
        diag_R=np.diag(np.ones(k))                                                    #R：约束矩阵
        location=len(diag_R)-1                                                        #最后一列的industry
        R=np.delete(diag_R,location,axis=1)
        R[location,-(len(industry_weight)-1):]=industry_weight['w'][:-1]
        W=R.dot(np.linalg.inv(R.T.dot(X.T).dot(V).dot(X).dot(R))).dot(R.T).dot(X.T).dot(V)       #Weights on each factor
        r=othorebal['dailyreturn']                                                               #Stocks' periodic return
        f=list(W.dot(r))
        return(f)
        
class Otho():
    def __init__(self):
        pass
    
    def Othogonization1(self,w):
        return w.dot(inv(sqrtm(w.T.dot(w))))

    def Othogonization2(self,w):
        return orth(w)

    def Gram_Schmidt(self,A):
    #Orthogonalize a set of vectors stored as the columns of matrix A."""
        # Get the number of vectors.
        n = A.shape[1]
        for j in range(n):
        # To orthogonalize the vector in column j with respect to the
        # previous vectors, subtract from it its projection onto
        # each of the previous vectors.
            for k in range(j):
                A[:, j] -= np.dot(A[:, k], A[:, j]) * A[:, k]
            A[:, j] = A[:, j] / np.linalg.norm(A[:, j])
        return A


    
    