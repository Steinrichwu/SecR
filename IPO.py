# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 11:10:56 2021

@author: wudi
"""
#IPO Analysis

import pandas as pd
from MSSQL import MSSQL
from Toolbox import DataCollect
ms = MSSQL(host="GS-UATVDBSRV01\GSUATSQL",user="sa",pwd="SASThom111",db="JYDBBAK")


DC=DataCollect()

class IPO():
    def __init__(self):
        pass
    
    #Download historical data for Ashares (ex-科创板)
    def IPO_Ashs(self):
        sql="select convert(varchar,AIPO.ListDate,23), SM.SecuCode,AIPO.FirstChangePCT,AIPO.FirstTurnover, AIPO.LotRateOnline, AIPO.LotRateLP,AIPO.IPOProceeds from JYDBBAK.dbo.LC_AShareIPO AIPO left join JYDBBAK.dbo.SecuMain SM on AIPO.InnerCode=SM.InnerCode where AIPO.ListDate>'2010-01-01'"
        reslist=ms.ExecQuery(sql)    
        rechist=pd.DataFrame(reslist,columns=['date','ticker','IPOChange','IPOTurnover','IPOLotRateOnLine','IPOLotRateOffLine','Proceeds'])
        rechist[['IPOChange','IPOTurnover','IPOLotRateOnLine','IPOLotRateOffLine','Proceeds']]=rechist[['IPOChange','IPOTurnover','IPOLotRateOnLine','IPOLotRateOffLine','Proceeds']].astype(float)
        return(rechist)
    
    #def Analysis(self,IPOhist,field):
     #   IPOhist=IPOhist.loc[IPOhist[field].isnull()==False,:].copy()
      #  IPOhist['Avg'+field]=IPOhist.groupby(['date'])[field].transform('median')
       # IPOhist=IPOhist.drop_duplicates(subset=['date'],keep='last')
        #IPOhist['30dAvg'+field]=IPOhist['Avg'+field].rolling(30).mean()
        #IPOhist=IPOhist[['date','30dAvg'+field]]
        #return(IPOhist)
    
    #Calculate last twelve months' total/median proceeds by sector/ last ten years' total proceeds and IPO deals count
    def Proceeds_Analysis(self,rebalday):
        IPOhist=self.IPO_Ashs()
        lastyeardate=str(int(rebalday[0][0:4])-1)+rebalday[0][4:]
        tickerlist=list(IPOhist['ticker'].unique())
        secname=DC.Sec_name('CSI')
        secname=secname.rename(columns={'sector':'primecode'})
        sechist=DC.Stock_sector(rebalday,tickerlist,'CSI')
        IPOhist=pd.merge(IPOhist,sechist[['ticker','primecode']],on='ticker',how='left')
        IPOhist=pd.merge(IPOhist,secname[['primecode','sectorname']],on='primecode',how='left')
        IPOlastyear=IPOhist.loc[IPOhist['date']>lastyeardate,:].copy()
        IPOlastyear['sector_proceeds_sum']=IPOlastyear.groupby('primecode')['Proceeds'].transform('sum')
        IPOlastyear['sector_procceds_median']=IPOlastyear.groupby('primecode')['Proceeds'].transform('median')
        IPOhist['year']=[str(x)[0:4] for x in IPOhist['date']]
        IPOhist['year_proceeds']=IPOhist.groupby('year')['Proceeds'].transform('sum')
        IPOhist['deals_count']=IPOhist.groupby('year')['Proceeds'].transform('count')
        IPOyearhist=IPOhist.drop_duplicates(subset=['year'],keep='last')
        IPOlastyear_sector=IPOlastyear.drop_duplicates(subset=['primecode'],keep='last')
        IPOyearhist=IPOyearhist[['year','year_proceeds','deals_count']]
        IPOlastyear_sector=IPOlastyear_sector[['sectorname','sector_proceeds_sum','sector_procceds_median']]
        IPOyearhist=IPOyearhist.loc[IPOyearhist['year']!='None',:].copy()
        IPOlastyear_sector=IPOlastyear_sector.loc[IPOlastyear_sector['sectorname'].isnull()==False,:].copy()
        IPOyearhist=IPOyearhist.sort_values(by=['year'],ascending=[True])
        #IPOlastyear_sector.to_csv("D:/CompanyData/IPO_TTM_"+todayname+".csv",encoding='utf-8-sig',index=False)
        #IPOyearhist.to_csv(":D/CompanyData/IPO_Hist_"+todayname+".csv",encoding='utf-8-sig',index=False)
        return(IPOlastyear_sector,IPOyearhist)
        
  