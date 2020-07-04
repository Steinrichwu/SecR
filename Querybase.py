# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 20:55:10 2020

@author: wudi
"""

#https://dd.gildata.com/#/tableShow/258/column//mine/LC_MainIndexNew (LC_MainINdexNdw 现成财务比率)
#LC_MainDataNew 是YTD数据，按公司财报结构分类的数字，Ratio
#LC_MainIndexNew 是YTD数据,按公司能力分类的Ratio
#QFI是季度数据/ QFI Mark=2: 合并未调整

class Query():
    def __init__(self):
            pass
    
    #Valuation    
    def Valuation_Reciprocal(self,signame,rebalday):
        sql="With Temp As (Select V.TradingDay, V."+signame+", V.InnerCode from JYDBBAK.dbo.LC_DIndicesForValuation V where V.TradingDay='"+rebalday+"') Select temp.TradingDay,temp.TradingDay,SM.SecuCode,1/temp."+signame+" from Temp left join JYDBBAK.dbo.SecuMain SM on Temp.InnerCode=SM.InnerCode where SM.SecuCategory = 1"
        return(sql)
        
    def ROECutYTD(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ROECut from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    def RevenueYOY(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.OperatingRevenueYOY from JYDBBAK.dbo.LC_QFinancialIndexNew QFI left join JYDBBAK.dbo.SecuMain SM  on QFI.CompanyCode=SM.CompanyCode where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    def NetProfitYOY(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.NetProfitYOY from JYDBBAK.dbo.LC_QFinancialIndexNew QFI left join JYDBBAK.dbo.SecuMain SM  on QFI.CompanyCode=SM.CompanyCode where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
        
    def ROETTM(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ROETTM from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"           
        return(sql)
        
    def ROATTM(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ROATTM from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"           
        return(sql)
    
    def GrossIncomeRatioTTM(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.GrossIncomeRatioTTM from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"           
        return(sql)
    
        
    def ROE(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.ROE from JYDBBAK.dbo.LC_QFinancialIndexNew QFI left join JYDBBAK.dbo.SecuMain SM  on QFI.CompanyCode=SM.CompanyCode where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    def ROA(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.ROA from JYDBBAK.dbo.LC_QFinancialIndexNew QFI left join JYDBBAK.dbo.SecuMain SM  on QFI.CompanyCode=SM.CompanyCode where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
        
    def OperatingProfitToTORTTM(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperatingProfitToTORTTM  from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"           
        return(sql)
        
    def OCF_Ratio(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NetProfitCashCover from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"           
        return(sql)
        
            
    #总资产周转率（TotalAssetTRate）＝营业总收入*2/（期初资产合计+期末资产合计）
    def TotalAssetTRate(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.TotalAssetTRate from JYDBBAK.dbo.LC_QFinancialIndexNew QFI left join JYDBBAK.dbo.SecuMain SM  on QFI.CompanyCode=SM.CompanyCode where SM.SecuCategory = 1 and QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    #LC_IncomeStatementAll: Ifmerged=1 合并报表，非母公司； IfAdjusted=2 未调整；  IncomeStatementALL也是YTD
    def RanDYTD(self,startdate):
        sql="Select ICS.InfoPublDate, ICS.EndDate, SM.SecuCode, ICS.RAndD from JYDBBAK.dbo.LC_IncomeStatementAll ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCategory=1 and ICS.IfAdjusted=2 and ICS.IfMerged=1 and ICS.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
        
    def QuickRatioYTD(self,startdate): #QFI, SM, MI (YTD)
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.QuickRatio from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory=1 AND QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    def GrossMargin(self,startdate):
        sql="Select QFI.InfoPublDate, QFI.EndDate, SM.Secucode, QFI.GrossIncomeRatio from  JYDBBAK.dbo.SecuMain SM  left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on QFI.CompanyCode=SM.CompanyCode where QFI.Mark=2 AND SM.SecuCategory=1 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
        
    def DebtAssetYTD(self,startdate): #QFI,SM,MI (YTD)
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.DebtAssetsRatio from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCategory=1 AND QFI.Mark=2 AND QFI.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)
    
    def QRevenue(self,startdate): #QIncomeStatementNew, report Quarterly data，用GrowthFunda计算出来和RevenueYOY in QFI吻合
        sql="Select ICS.InfoPublDate, ICS.EndDate, SM.SecuCode, ICS.OperatingRevenue from JYDBBAK.dbo.LC_QIncomeStatementNew ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCategory=1 AND ICS.Mark=2 and ICS.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)

    def QNetprofit(self,startdate): #Quarterly Netprofit要看归母净利润才对,用GrowthFunda计算出来和NetProfitYOY in QFI吻合
        sql="Select ICS.InfoPublDate, ICS.EndDate, SM.SecuCode, ICS.NPFromParentCompanyOwners from JYDBBAK.dbo.LC_QIncomeStatementNew ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCategory=1 AND ICS.Mark=2 and ICS.InfoPublDate>=DATEADD(year,-3,'"+startdate+"')"
        return(sql)

    









#Profitability-II (Return on Sales)
    def OperatingProfitMargin(self,tickerlist):
        sql="SELECT QFI.InfoPublDate, QFI.EndDate, SM.SecuCode, QFI.OperatingProfitMargin FROM JYDBBAK.dbo.SecuMain SM LEFT JOIN JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.CompanyCode=SM.CompanyCode) WHERE SM.SecuCode  in ("+str(tickerlist)[1:-1]+") AND (SM.SecuCategory=1) AND (QFI.Mark=2) AND (QFI.InfoPublDate>='2009-01-01')"
        return(sql)
        
    
    
    def NetProfitMargin(self, tickerlist): #QFI+SM
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, QFI.NetProfitRatio from  JYDBBAK.dbo.SecuMain SM  left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on QFI.CompanyCode=SM.CompanyCode where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
    
    def EBITMargin(self,tickerlist): #QFI+MI+SM
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EBITToTOR from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    def OperatingProfitToTOR(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperatingProfitToTOR from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
        
#CashFlow Ratios -I (Performance)    
    #Operating cash flow per shares/sales pershares
    def CFORevenue1(self,tickerlist): 
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashFlowPS/NULLIF(MI.TotalOperatingRevenuePS,0) from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    def CFORevenue2(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.CashRateOfSales from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
    
    #Operating cash flow per shas/net asset per shares
    def CFONA(self,tickerlist): #QFI, MI, SM
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashFlowPS/NULLIF(MI.NetAssetPS,0) from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    #Operating cash flow per shs/total asset
    def CFOTA(self,tickerlist): #CF,BS,SM
        sql="Select BS.InfoPublDate,BS.EndDate, SM.SecuCode, CF.NetOperateCashFlow/NULLIF(BS.TotalAssets,0) from JYDBBAK.dbo.LC_BalanceSheetAll BS left join JYDBBAK.dbo.SecuMain SM on BS.CompanyCode=SM.CompanyCode left join JYDBBAK.dbo.LC_CashFlowStatementAll CF on (BS.CompanyCode=CF.CompanyCode and BS.EndDate=CF.EndDate) where (SM.SecuCode in ("+str(tickerlist)[1:-1]+") and BS.IfMerged=1 and CF.IfMerged=1 and BS.IfAdjusted=2 and CF.IfAdjusted=2 and BS.InfoPublDate>='2009-01-01')"
        return(sql)
        
    def CFOTA2(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashInToAsset from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
    
    #Operating cash flow / operating income per shares
    def CFOOI1(self,tickerlist): #QFI,MI,SM
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashFlowPS/NULLIF(MI.OperProfitPS,0) from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
   
    def CFOOI2(slef,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NOCFToOperatingNI from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
        #% of operating cash vs Net Profit
    def NetProfitCashCover(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NetProfitCashCover from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return (sql)
        
        #% of operating cash vs Operating Revenue
    def OperatingRevenueCashCover(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperatingRevenueCashCover from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return (sql)
    
#CashFlow Ratios -II (Coverage)
    def OperatingCFToLiability(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NOCFToTLiability from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    def CFOCurrentLiability(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NOCFToCurrentLiability from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
  

#Solvency Ratio:
    def CashRatio(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashInToCurrentDebt from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)

    
    
    def CurrentRatio(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.CurrentRatio from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)

    def EBITDAToLiability(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EBITDAToTLiability from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)

#Leverage:
    def DebtAsset(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.DebtAssetsRatio from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
    
    def LTDebtRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.LongDebtToAsset from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    def EquityTOAsset(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EquityToAsset from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
        #固定资产比率
    def FixAssetRatio(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.FixAssetRatio from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
    def CurrentLiabilityRatio(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.CurrentLiabilityToTL from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
        #权益乘数（EquityMultipler）＝资产合计/股东权益合计
    def EquityMultipler(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EquityMultipler from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
#Activity Ratios:
    #总资产周转率
    def AssetTurnover(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.TotalAssetTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"
        return(sql)
        
        #存货周期率（次） Inventory Turnover=Cost of Sales/Avernage Inventory
    def InventoryTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.InventoryTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'" 
        return(sql)
        
        #存货周期（天） Days of Inventory on hand=Number of days in period/Inventory turnover
    def InventoryTDays(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.InventoryTDays from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"   
        return(sql)
        
        #应收账款周转率 Receivables turnover=Revenue/Average receivables
    def ARTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ARTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"   
        return(sql)
        
        #应收账款周转率 Number of days in period/Receivables turnover
    def ARTDays(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ARTDays from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"      
        return(sql)
        
        #应付账款周转率(次) Purchase/Average trade payables
    def AccountsPayablesTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.AccountsPayablesTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"          
        return(sql)
    
        #应付账款周转（天） Number of days in period/Payable turnover
    def AccountsPayablesTDays(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.AccountsPayablesTDays from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #流动资产周转率(次) =营业总收入*2/（期初流动资产合计+期末流动资产合计）
    def CurrentAssetsTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.CurrentAssetsTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #固定资产周转率(次) ＝营业总收入*2/（期初固定资产合计+期末固定资产合计
    def FixedAssetTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.FixedAssetTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #股东权益周转率(次) ＝营业总收入*2/（期初净资产+期末净资产）
    def EquityTRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EquityTRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        

#Self-constructed Growth
    #扣非净利润YTD (MainDataNew都是YTD)
    def NetProfitCutYTD(self,tickerlist):
        sql="Select MDN.InfoPublDate, MDN.EndDate, SM.SecuCode, MDN.NetProfitCut from JYDBBAK.dbo.LC_MainDataNew MDN left join JYDBBAK.dbo.SecuMain SM on MDN.CompanyCode=SM.CompanyCode where SM.SecuCode in  ("+str(tickerlist)[1:-1]+") and MDN.InfoPublDate>='2009-01-01' and MDN.Mark=2"
        return(sql)
        
    #营业收入（及增长) #这个和QIS里的Revenue是同一个数
    def TotalOperatingRevenue(self,tickerlist):
        sql="Select ICS.InfoPublDate, ICS.EndDate, SM.SecuCode, ICS.OperatingRevenue from JYDBBAK.dbo.LC_IncomeStatementAll ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCode in  ("+str(tickerlist)[1:-1]+") and ICS.IfMerged=1 and ICS.IfAdjusted=2 and ICS.InfoPublDate>='2009-01-01'"
        return(sql)
    
    #净利润（及增长）    
    def NetProfit(self,tickerlist):
        sql="Select ICS.InfoPublDate, ICS.EndDate, SM.SecuCode, ICS.NetProfit from JYDBBAK.dbo.LC_IncomeStatementAll ICS left join JYDBBAK.dbo.SecuMain SM on ICS.CompanyCode=SM.CompanyCode where SM.SecuCode in  ("+str(tickerlist)[1:-1]+") and ICS.IfMerged=1 and ICS.IfAdjusted=2 and ICS.InfoPublDate>='2009-01-01'"
        return(sql)
    
    #每股收益（及增长）
    def EPS(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EPS from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    #每股息税前利润(元/股)
    def EBITPS(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EBITPS from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    #每股现金流量净额（及增长）
    def CashFlowPS(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.CashFlowPS from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    #每股经营现金流（及增长）
    def OperCashFlowPS(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperCashFlowPS from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    #自由现金流
    def FreeCashFlow(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.FreeCashFlow from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    #每股分红      
    def DividendPS(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.DividendPS from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
#Growth (NonSelfbuilt)
    def DilutedEPSYOY(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.DilutedEPSYOY from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
    def RevenueGrowth(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperatingRevenueGrowRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    def GrossProfitGrowth(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.OperProfitGrowRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    def GrossProfit3Y(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.ORComGrowRate3Y from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    def NIGrowth(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NetProfitGrowRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
    def EPSGrowRateYTD(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EPSGrowRateYTD from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
    def TotalAssetGrowRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.TotalAssetGrowRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    
    def NetAssetGrowRate(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NetAssetGrowRate from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)

#DuPont Analysis
        #权益乘数_杜邦分析
    def EquityMultipler_Dupont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EquityMultipler_Dupont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #归属母公司股东的净利润/净利润
    def NPPCToNP_DuPont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NPPCToNP_DuPont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #净利润/营业总收入
    def NPToTOR_DuPont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NPToTOR_DuPont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #净利润/利润总额
    def NPToTP_DuPont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.NPToTP_DuPont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #利润总额/息税前利润(
    def TPToEBIT_DuPont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.TPToEBIT_DuPont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
        
        #息税前利润/营业总收入
    def EBITToTOR_DuPont(self,tickerlist):
        sql="Select QFI.InfoPublDate, QFI.EndDate,SM.Secucode, MI.EBITToTOR_DuPont from JYDBBAK.dbo.LC_MainIndexNew MI left join JYDBBAK.dbo.SecuMain SM on SM.CompanyCode=MI.CompanyCode left join JYDBBAK.dbo.LC_QFinancialIndexNew QFI on (QFI.EndDate=MI.EndDate and QFI.CompanyCode=MI.CompanyCode) where SM.SecuCode in ("+str(tickerlist)[1:-1]+") and QFI.Mark=2 AND QFI.InfoPublDate>='2009-01-01'"           
        return(sql)
    

  
#Analysts related
    def Activeanalyst(self,rebalday):
        sql="select DISTINCT analyst_id from MODEL_TRADING where date>DATEADD(year,-1,'"+rebalday+"') and date<'"+rebalday+"'"
        return(sql)
    
    def Top25pct_analyst(self,rebalday,lookbackday,analystuniverse):
        sql="Select TOP 25 PERCENT MP.analyst_id, SUM(MP.dtd_chg) AS CumPNL from hyzb.dbo.MODEL_PORTFOLIO MP where MP.date>='"+lookbackday+"' and MP.date<'"+rebalday+"' and MP.analyst_id in ("+str(analystuniverse)[1:-1]+") GROUP BY MP.analyst_id ORDER BY CumPNL DESC"
        return(sql)
        
    def Analyst_holding(self,rebalday,analystuniverse):
        sql="Select DISTINCT MH.ticker from hyzb.dbo.MODEL_HOLDING MH where MH.date='"+rebalday+"' and MH.analyst_id in ("+str(analystuniverse)[1:-1]+")"
        return(sql)
    
    def Top5_analyst(self,rebalday,lookbackday,analystuniverse):
        sql="Select TOP 5 MP.analyst_id, SUM(MP.dtd_chg) AS CumPNL from hyzb.dbo.MODEL_PORTFOLIO MP where MP.date>='"+lookbackday+"' and MP.date<'"+rebalday+"' and MP.analyst_id in ("+str(analystuniverse)[1:-1]+") GROUP BY MP.analyst_id ORDER BY CumPNL DESC"
        return(sql)
    
    #download the 60day rolling return of analysts' protfolio and rolling return of their benchmark (all history and all analsyts)
    #****Verified*****
    def Analyst_alpha(self):
        sql="select MP.date, MP.analyst_id, SUM(MP.dtd_chg) OVER (PARTITION by MP.analyst_id Order by date rows between 59 preceding and current row)as CumReturn, SUM(MP.benchmark_dtd_chg) OVER (PARTITION by MP.analyst_id Order by date rows between 59 preceding and current row)as CumBenchmark from hyzb.dbo.MODEL_PORTFOLIO MP where  IsNumeric(analyst_id) = 1 ORDER BY analyst_id "
        return(sql)
        
    #Analysts' cumPNL of a period 
    def Analyst_cumPNL(self,rebalday,lookbackday,analystuniverse):
        sql="Select MP.analyst_id, SUM(MP.dtd_chg) AS CumPNL from hyzb.dbo.MODEL_PORTFOLIO MP where MP.date>='"+lookbackday+"' and MP.date<'"+rebalday+"' and MP.analyst_id in ("+str(analystuniverse)[1:-1]+") GROUP BY MP.analyst_id ORDER BY CumPNL DESC"
        return(sql)
        
    def Analyst_cumPNLnew(self,rebaldaylist):
        sql="Select convert(varchar,MP.date, 23) as date,  MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 59 preceding and current row) as CumPNL from hyzb.dbo.MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1 and date in ("+str(rebaldaylist)[1:-1]+")"
        return(sql)
        
    def Active_analyst1y(self,rebaldaylist):
        sql="Select P.date, P.analyst_id,P.CumPNL from (Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 10 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP WHERE ISNUMERIC(MP.analyst_id)=1) P where P.CumPNL<>0 and P.date in ("+str(rebaldaylist)[1:-1]+")"
        return(sql)
        
    def Active_analyst_test(self,rebaldaylist):
        sql="Select P.date, P.analyst_id,P.CumPNL from (Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 10 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP WHERE ISNUMERIC(MP.analyst_id)=1) P where P.date in ("+str(rebaldaylist)[1:-1]+")"
        return(sql)
    
    def Top30pct_analyst(self,rebaldaylist,active_analystlist):
        sql="SELECT convert(varchar,R.date,23), R.analyst_id,R.CumPNL,R.PercentRank from(Select P.date,P.analyst_id,P.CumPNL, PERCENT_RANK() OVER (Partition by P.date ORDER BY P.CumPNL DESC)*100 AS PercentRank from(Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 59 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1) P where P.date in ("+str(rebaldaylist)[1:-1]+") and P.analyst_id in ("+str(active_analystlist)[1:-1]+") and P.CumPNL<>0) R where R.PercentRank<30 order by R.date ASC"
        return(sql)
    
    def Top30pct_analyst_test(self,rebaldaylist,active_analystlist):
        sql="WITH temptable as (Select P.date,P.analyst_id,P.CumPNL, PERCENT_RANK() OVER (Partition by P.date ORDER BY P.CumPNL DESC)*100 AS PercentRank from(Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 59 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1) P where P.date in ("+str(rebaldaylist)[1:-1]+") and P.analyst_id in ("+str(active_analystlist)[1:-1]+") and P.CumPNL<>0) SELECT date,analyst_id,CumPNL,PercentRank from temptable where temptable.PercentRank<=30"
        return(sql)
        
        
    def Analyst_holding_rebaldaylist(self,sqlpart):
        sql="Select convert(varchar,MH.date,23), MH.analyst_id,MH.ticker from hyzb.dbo.MODEL_HOLDING MH where "+sqlpart+" order by date ASC"
        return(sql)
        
    def Top5_sector_analyst(self,rebaldaylist,sec_analystlist):
        sql="SELECT R.date, convert(varchar,R.date,23), R.analyst_id,R.CumPNL,R.Ranking from(Select P.date,P.analyst_id,P.CumPNL,ROW_NUMBER() OVER (Partition by P.date ORDER BY P.CumPNL DESC) as Ranking from(Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 59 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1) P where P.date in ("+str(rebaldaylist)[1:-1]+") and P.analyst_id in  ("+str(sec_analystlist)[1:-1]+") and P.CumPNL<>0) R where R.Ranking<=5"
        return(sql)
        
    def Top5_sector_analyst_test(self,rebaldaylist,sec_analystlist):
        sql="Select convert(varchar,P.date,23) as date,P.analyst_id,P.CumPNL,ROW_NUMBER() OVER (Partition by P.date ORDER BY P.CumPNL DESC) as Ranking from(Select MP.analyst_id, SUM(MP.dtd_chg) over (partition by MP.analyst_id order by date rows between 59 preceding and current row) as CumPNL, MP.date from hyzb.dbo.MODEL_PORTFOLIO MP where ISNUMERIC(MP.analyst_id)=1) P where P.date in ("+str(rebaldaylist)[1:-1]+") and P.analyst_id in  ("+str(sec_analystlist)[1:-1]+") and P.CumPNL<>0"
        return(sql)

#Hotstocks related
    #Choose a past n day x most recommened stocks, to be connected
    def Hotstock_nonsector(self,rebalday,xstocks,lookback_period):
        sqlpart="Select A.* from (select Top "+str(xstocks)+" with ties '"+rebalday+"' as rebalday, code, count(*) Reccount from jyzb_new_1.dbo.cmb_report_research R left join jyzb_new_1.dbo.I_SYS_CLASS C on C.SYS_CLASS=R.score_id left join jyzb_new_1.dbo.I_ORGAN_SCORE S on S.ID=R.organ_score_id where into_date>=dateadd(day,-"+str(lookback_period)+",'"+rebalday+"') and into_date<'"+rebalday+"' and (sys_class=7 OR sys_class=5) GROUP BY code ORDER BY Reccount DESC) A "
        return(sqlpart)

        

        