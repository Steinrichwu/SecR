# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 15:41:15 2020

@author: wudi
"""
import numpy as np
from scipy.optimize import minimize
from scipy.linalg import orth
from scipy.linalg import sqrtm, inv
    
def Optimize(port,bm):
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
        return s
    
    def fac_exposure_objective(weights):
        return-statistics(weights)
  
    cons=({'type':'eq','fun': lambda x: np.sum(x)-1},
          {'type':'eq','fun': lambda x:-np.linalg.norm(np.dot(x,Adummymatirx)-Cindustryexp)})
    bnds=tuple((0.001,0.05) for x in range(stock_num))    
    res=minimize(fac_exposure_objective,[0]*stock_num,constraints=cons,bounds=bnds,method='SLSQP')
    return(res['x'])
    
    
def Othogonization1(w):
    return w.dot(inv(sqrtm(w.T.dot(w))))

def Othogonization2(w):
    return orth(w)

def Gram_Schmidt(A):
    """Orthogonalize a set of vectors stored as the columns of matrix A."""
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