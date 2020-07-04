# -*- coding: utf-8 -*-
"""
Created on Tue May 19 22:43:49 2020

@author: wudi
"""

import pandas as pd
import numpy as np
import pymssql
import sys
import datetime
import time
from tqdm import tqdm
import statsmodels.api as sm
import math
import empyrical as ep

import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.style.use('seaborn-white')
mpl.rcParams['figure.figsize'] = (15,10)

import sys
class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db,
                                    charset="cp936")

    def __GetConnect(self):
        if not self.db:
            raise (NameError, "?????????")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "???????")
        else:
            return cur

    def ExecQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        return resList

    def ExecNonQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        return cur

    def Commit(self):
        self.conn.commit()

    def CloseConnect(self):
        self.conn.close()

print(sys.getdefaultencoding())

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql = """
     select TradingDate from QT_TradingDayNew WHERE SecuMarket = '83' 
     AND TradingDate >= '2004-01-01' and TradingDate <= '2020-04-30' AND IfTradingDay = '1' ORDER BY TradingDate"""
tradingday = ms.ExecNonQuery(sql)
tradingday = pd.DataFrame(tradingday)
tradingday.columns = ['tradingday']
early2 = tradingday.set_index('tradingday')

# ROE TTM
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql = """
select SM.SecuCode,QFI.InfoPublDate,MI.ROETTM from LC_MainIndexNew as MI 
JOIN SecuMain AS SM ON SM.CompanyCode = MI.CompanyCode
JOIN LC_QFinancialIndexNew AS QFI ON QFI.CompanyCode = MI.CompanyCode AND QFI.EndDate = MI.EndDate
WHERE SM.SecuCategory = '1' and SM.ListedState = '1' and SM.ListedSector in ('1','2','6')  
AND QFI.Mark = '2'  AND QFI.INFOPUBLDATE > '2004-01-01' ORDER BY SM.SECUCODE,QFI.InfoPublDate

"""
ROE_TTM = ms.ExecNonQuery(sql)
ROE_TTM = pd.DataFrame(ROE_TTM)
ROE_TTM.columns = ['secucode','date','ROE_TTM']
ROE_TTM['ROE_TTM'] = ROE_TTM['ROE_TTM'].astype('float')
ROE_TTM1 = ROE_TTM.pivot_table(index = 'date', columns = 'secucode',values = 'ROE_TTM').fillna(method = 'ffill')
ROE_TTM1 = ROE_TTM1.reindex(early2.index)
ROE_TTM1 = ROE_TTM1.fillna(method = 'ffill')

# PE
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql = """
select SM.SecuCode,MI.TradingDay,MI.PE from LC_DIndicesForValuation as MI 
JOIN SecuMain AS SM ON SM.InnerCode = MI.InnerCode
WHERE SM.SecuCategory = '1' and SM.ListedState = '1' and SM.ListedSector in ('1','2','6')  
AND TradingDay > '2004-01-01' ORDER BY SM.SECUCODE,TradingDay
"""
PE = ms.ExecNonQuery(sql)
PE = pd.DataFrame(PE)
PE.columns = ['secucode','date','PE']
PE['PE'] = PE['PE'].astype('float')
PE1 = PE.pivot_table(index = 'date', columns = 'secucode',values = 'PE').fillna(method = 'ffill')
PE1 = PE1.reindex(early2.index)
PE1 = PE1.fillna(method = 'ffill')
EP = 1/PE1

# Operating Revenue YOY
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql ="""
select SM.SecuCode,QFI.InfoPublDate,MI.OperatingRevenueYOY from LC_QFinancialIndexNew as MI 
JOIN SecuMain AS SM ON SM.CompanyCode = MI.CompanyCode
JOIN LC_QFinancialIndexNew AS QFI ON QFI.CompanyCode = MI.CompanyCode AND QFI.EndDate = MI.EndDate
WHERE SM.SecuCategory = '1' and SM.ListedState = '1' and SM.ListedSector in ('1','2','6')  
AND QFI.Mark = '2'  AND QFI.INFOPUBLDATE > '2004-01-01' AND MI.MARK = '2'
ORDER BY SM.SECUCODE,QFI.InfoPublDate
"""
ORG = ms.ExecNonQuery(sql)
ORG = pd.DataFrame(ORG)
ORG.columns = ['secucode','date','ORG']
ORG['ORG'] = ORG['ORG'].astype('float')
ORG1 = ORG.pivot_table(index = 'date', columns = 'secucode',values = 'ORG').fillna(method = 'ffill')
ORG1 = ORG1.reindex(early2.index)
ORG1 = ORG1.fillna(method = 'ffill')

def get_industrylist():
    information = pd.read_excel('D:/bridgetzhang/anaconda/projects/niutwo/information.xlsx',index_col = 0)
    ind_list = information['industryname']
    return ind_list

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql = """
     select qt.TradingDay,sm.SecuCode,qt.NegotiableMV from QT_Performance as qt
    join secumain as sm on sm.innercode = qt.innercode where qt.TradingDay > '2009-01-01'"""
totalmv = ms.ExecNonQuery(sql)
totalmv = pd.DataFrame(totalmv)
totalmv.columns = ['tradingday','code','mv']
totalmv['mv'] = totalmv['mv'].astype('float')
totalmv = totalmv.pivot_table(index='tradingday',columns = 'code',values = 'mv')
totalmv = totalmv.fillna(method = 'ffill')

ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
sql = """select s.secucode,q.ChangePCT,q.TradingDay from QT_Performance q 
        left join secumain s on q.innercode = s.innercode
        where s.secucode in ('"""+ ("','").join(totalmv.columns.tolist())+\
        "') and q.TradingDay >='2009-01-01'"

changeper1 = ms.ExecNonQuery(sql)
changeper1 = pd.DataFrame(changeper1)
changeper1.columns = ['code','changeper','date']
changeper1 = changeper1.set_index('date')
changeper1['changeper'] = changeper1['changeper'].astype('float')
changeper1 = changeper1.pivot_table(index = 'date',columns = 'code',values = 'changeper')
changeper1 *= 0.01

def neutralization(ROE, mkt_cap, ind_map, ind_list):
    cap = pd.DataFrame(index=mkt_cap.index, columns=['MV'], data=[math.log(i) for i in mkt_cap.values])
    cap = cap.loc[ROE.index,:]

    industry = pd.DataFrame(0, columns=ROE.index, index=range(0, 29))
    for i in ROE.index:
        try:
            industry.loc[ind_list[ind_list == ind_map.loc[i, 'industryname']].index[0], i] = 1
        except KeyError:
            pass
    ROE = ROE.replace(np.inf,np.nan).replace(-np.inf,np.nan)
    conc = pd.concat([ROE, cap, industry.T], axis=1).dropna()
    est = sm.OLS(conc.iloc[:, 0], conc.iloc[:, 1:]).fit()
    re = pd.DataFrame(index=est.resid.keys(), columns=['new_factor'], data=est.resid.values)

    return re

def get_tradingday(start_date,end_date):
    return early2.loc[start_date:end_date]

def map_secu2ind():
    ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")
    sql =""" 
            select SecuMain.SecuCode,LC_ExgIndustry.firstindustryname 
            from LC_ExgIndustry,SecuMain where LC_ExgIndustry.standard = '3' and LC_ExgIndustry.ifperformed = '1' 
            and SecuMain.CompanyCode = LC_ExgIndustry.CompanyCode and SecuMain.SecuCategory = '1' """
    ind_map = ms.ExecNonQuery(sql)
    ind_map = pd.DataFrame(ind_map)
    ind_map.columns = ['secucode','industryname']
    ind_map = ind_map.set_index('secucode')
    return ind_map




def longshort_port(group, start_date, end_date, factor_list, holding):

    ind_map = map_secu2ind()
    ind_list = get_industrylist()
    mkt_cap = totalmv.loc[start_date:, factor_list.columns]
    rebalance_day = [early2.loc[start_date:end_date].index[i] for \
                     i in range(len(early2.loc[start_date:end_date])) if i % holding == 0]
    tt_dict = dict()
    for i in tqdm(range(len(rebalance_day))):
        rebal_day = rebalance_day[i]
        factor = factor_list.loc[rebal_day, :]
        factor = pd.DataFrame(index=factor.index, columns=['ROE'],
                              data=factor.values.reshape(len(factor.index), 1).tolist())
        mkt_cap_q = mkt_cap.loc[rebal_day, :]

        factor_new = neutralization(factor, mkt_cap_q, ind_map, ind_list)
        factor_new = factor_new.dropna()
        cutRes_q = pd.qcut(factor_new['new_factor'], group, labels=pd.np.arange(1, group + 1))
        tt_dict[rebal_day] = cutRes_q.copy()

    return tt_dict

start_date = '2010-01-01'
end_date = '2020-04-30'
ROE_qcut = longshort_port(5,start_date,end_date,ROE_TTM1,20)
ORG_qcut = longshort_port(5,start_date,end_date,ORG1,20)
EP_qcut = longshort_port(5,start_date,end_date,EP,20)

rebalance_day = [early2.loc[start_date:end_date].index[i] for \
                     i in range(len(early2.loc[start_date:end_date])) if i%20 == 0]

# selected_dict = dict()
group5_dict = dict()
group4_dict = dict()
group3_dict = dict()
group2_dict = dict()
group1_dict = dict()

for i in rebalance_day:
    temp_ROE = ROE_qcut[i]
    temp_ORG = ORG_qcut[i]
    temp_EP = EP_qcut[i]
    temp_stock = list(set(ROE_qcut[i].index.tolist() + ORG_qcut[i].index.tolist() + EP_qcut[i].index.tolist()))
    temp_qcut = pd.DataFrame(index=temp_stock, columns=['ROE', 'ORG', 'EP'])
    temp_qcut.loc[temp_ROE.index, 'ROE'] = temp_ROE.loc[:]
    temp_qcut.loc[temp_ORG.index, 'ORG'] = temp_ORG.loc[:]
    temp_qcut.loc[temp_EP.index, 'EP'] = temp_EP.loc[:]
    final_qcut = pd.qcut(temp_qcut.mean(1), 5, labels=pd.np.arange(1, 6))

    group5_dict[i] = final_qcut[final_qcut == 5].index.tolist()
    group4_dict[i] = final_qcut[final_qcut == 4].index.tolist()
    group3_dict[i] = final_qcut[final_qcut == 3].index.tolist()
    group2_dict[i] = final_qcut[final_qcut == 2].index.tolist()
    group1_dict[i] = final_qcut[final_qcut == 1].index.tolist()

total_return = pd.DataFrame(index = early2.loc[start_date:end_date].index,columns = ['group1','group2','group3',\
                                                                                          'group4','group5'])

import datetime as dt

for i in range(len(rebalance_day)):
    rebal_date = rebalance_day[i]
    begin_date = pd.to_datetime(rebal_date) + dt.timedelta(days=+1)
    temp_5 = group5_dict[rebal_date]
    temp_4 = group4_dict[rebal_date]
    temp_3 = group3_dict[rebal_date]
    temp_2 = group2_dict[rebal_date]
    temp_1 = group1_dict[rebal_date]

    if i < len(rebalance_day) - 1:
        end_date = rebalance_day[i + 1]

        total_return.loc[begin_date:end_date, 'group5'] = (changeper1.loc[begin_date:end_date, temp_5]).mean(1)
        total_return.loc[begin_date:end_date, 'group4'] = (changeper1.loc[begin_date:end_date, temp_4]).mean(1)
        total_return.loc[begin_date:end_date, 'group3'] = (changeper1.loc[begin_date:end_date, temp_3]).mean(1)
        total_return.loc[begin_date:end_date, 'group2'] = (changeper1.loc[begin_date:end_date, temp_2]).mean(1)
        total_return.loc[begin_date:end_date, 'group1'] = (changeper1.loc[begin_date:end_date, temp_1]).mean(1)

    else:

        total_return.loc[begin_date:, 'group5'] = (changeper1.loc[begin_date:, temp_5]).mean(1)
        total_return.loc[begin_date:, 'group4'] = (changeper1.loc[begin_date:, temp_4]).mean(1)
        total_return.loc[begin_date:, 'group3'] = (changeper1.loc[begin_date:, temp_3]).mean(1)
        total_return.loc[begin_date:, 'group2'] = (changeper1.loc[begin_date:, temp_2]).mean(1)
        total_return.loc[begin_date:, 'group1'] = (changeper1.loc[begin_date:, temp_1]).mean(1)

(1+total_return).cumprod().plot()
plt.show()