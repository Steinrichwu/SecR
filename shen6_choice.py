import numpy as np
import pandas as pd
from EmQuantAPI import *
from datetime import timedelta, datetime
import time as _time
import traceback


def mainCallback(quantdata):
    """
    mainCallback 是主回调函数，可捕捉如下错误
    在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("mainCallback", str(quantdata))
    # 登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
    if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
        print("Your account is disconnect. You can force login automatically here if you need.")
    # 行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
        print("Your all csq subscribe have stopped.")
    # 行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002009":
        print("Your all csq subscribe have stopped, reconnect 6 times fail.")
        # 行情订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_QUOTE_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002012":
        print("csq subscribe break on some error, reconnect and request automatically.")
        # 资讯服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002014":
        print("Your all cnq subscribe have stopped, reconnect 6 times fail.")
    # 资讯订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_INFO_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002013":
        print("cnq subscribe break on some error, reconnect and request automatically.")
    # 资讯登录验证失败（每次连接资讯服务器时需要登录验证）或者资讯流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001024" or str(quantdata.ErrorCode) == "10001025":
        print("Your all cnq subscribe have stopped.")
    else:
        pass


def startCallback(message):
    print("[EmQuantAPI Python]", message)
    return 1


def csqCallback(quantdata):
    """
    csqCallback 是csq订阅时提供的回调函数模板。该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("csqCallback,", str(quantdata))


def cstCallBack(quantdata):
    '''
    cstCallBack 是日内跳价服务提供的回调函数模板
    '''
    for i in range(0, len(quantdata.Codes)):
        length = len(quantdata.Dates)
        for it in quantdata.Data.keys():
            print(it)
            for k in range(0, length):
                for j in range(0, len(quantdata.Indicators)):
                    print(quantdata.Data[it][j * length + k], " ", end="")
                print()


def cnqCallback(quantdata):
    """
    csqCallback 是cnq订阅时提供的回调函数模板。该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    # print ("cnqCallback,", str(quantdata))
    print("cnqCallback,")
    for code in quantdata.Data:
        total = len(quantdata.Data[code])
        for k in range(0, len(quantdata.Data[code])):
            print(quantdata.Data[code][k])

# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 11:08:21 2021

@author: wudi
"""
loginResult = c.start("ForceLogin=1", '', mainCallback)

if (loginResult.ErrorCode != 0):
    print("login in fail")
    exit()

trade_date = '2021-01-05'
#1.获取所有公募基金名单
# data=c.sector("507013",trade_date)
# fund_list = pd.DataFrame(index = [data.Data[i] for i in range(len(data.Data)) if i%2 ==0],columns =['名称'], \
#                          data = [data.Data[i] for i in range(len(data.Data)) if i%2 ==1])

#所有基金有10000+，现以Choice中消费行业板块基金为例
data=c.sector("516002031",trade_date)
fund_list = pd.DataFrame(index = [data.Data[i] for i in range(len(data.Data)) if i%2 ==0],columns =['名称'], \
                         data = [data.Data[i] for i in range(len(data.Data)) if i%2 ==1])

#2.基金经理信息
# 任职期限最长的现任基金经理 学历 简历 任职天数
fund_manager = c.css((',').join(n for n in fund_list.index),\
                     "MGRLONGESTYEARS,MGRDEGREE,MGRRESUME,MGRDATES","Rank=1,IsPandas=1")

#3.基金单位净值、日内涨跌幅、换手率(只有场内基金才有涨跌幅和换手率数据)
fund_perf = c.csd((',').join(n for n in fund_list.index),"UNITNAV,PCTCHANGE,TURN","2010-01-01",trade_date,\
      "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH,IsPandas=1")
fund_perf[['UNITNAV','PCTCHANGE','TURN']] = fund_perf[['UNITNAV','PCTCHANGE','TURN']].astype('float')
unitnav = fund_perf.pivot_table(index = 'DATES',columns = 'CODES', values = 'UNITNAV')
pct_change = fund_perf.pivot_table(index = 'DATES',columns = 'CODES', values = 'PCTCHANGE')
turnover = fund_perf.pivot_table(index = 'DATES',columns = 'CODES', values = 'TURN')

#4.基金前十大重仓股
quarter_date = ['03-31','06-30','09-30','12-31']
total_holding = pd.DataFrame(columns = [n for n in range(1,11)]+['report_date','publ_date'])
for i in range(2010,2021):
    for j in quarter_date:
        temp_report = str(i)+'-'+j
        temp_holding = pd.DataFrame(index = fund_list.index,columns = [n for n in range(1,11)])
        for n in range(1,11):
            temp_data = c.css((',').join(n for n in fund_list.index),"PRTKEYSTOCKCODE","ReportDate="+temp_report+",Rank="+\
                              str(n)+",IsPandas=1")
            temp_holding[n] = temp_data['PRTKEYSTOCKCODE']
        temp_holding['report_date'] = temp_report
        temp_holding['publ_date'] = \
        c.css((',').join(n for n in fund_list.index),"QUATERSTMTDATE","ReportDate="+temp_report+",IsPandas=1")['QUATERSTMTDATE']
        total_holding = pd.concat([total_holding,temp_holding])

#5.重仓行业名称 报告期末持有股票个数 大盘价值股风格占比
from tqdm import tqdm
fund_info = pd.DataFrame(columns = ['重仓行业名称','报告期末持有股票个数','大盘价值风格占比','大盘混合型风格占比','大盘成长型风格占比',\
                                   '中盘盘价值风格占比','中盘混合型风格占比','中盘成长型风格占比',\
                                    '小盘盘价值风格占比','小盘混合型风格占比','小盘成长型风格占比','report_date'])
style = ['大盘价值风格占比','大盘混合型风格占比','大盘成长型风格占比',\
        '中盘盘价值风格占比','中盘混合型风格占比','中盘成长型风格占比',\
        '小盘盘价值风格占比','小盘混合型风格占比','小盘成长型风格占比']
for i in tqdm(range(2010,2021)):
    for j in quarter_date:
        temp_info = pd.DataFrame(index = fund_list.index,columns = ['重仓行业名称','报告期末持有股票个数','大盘价值风格占比',\
                                                                    '大盘混合型风格占比','大盘成长型风格占比',\
                                                               '中盘盘价值风格占比','中盘混合型风格占比','中盘成长型风格占比',\
                                                    '小盘盘价值风格占比','小盘混合型风格占比','小盘成长型风格占比','report_date'])
        temp_report = str(i)+'-'+j
        temp_info['report_date'] = temp_report
        temp_info['重仓行业名称'] = c.css((',').join(n for n in fund_list.index),\
                                    "PRTKEYINDNAME","ReportDate={},Rank=1,IsPandas=1".format(temp_report))['PRTKEYINDNAME']
        temp_info['报告期末持有股票个数'] = c.css((',').join(n for n in fund_list.index),\
                                    "PRTSTOCKNUM","ReportDate={},Rank=1,IsPandas=1".format(temp_report))['PRTSTOCKNUM']
        for n in range(0,9):
            temp_info[style[n]] = c.css((',').join(n for n in fund_list.index),\
                                    "STYLERATE","ReportDate={},StyleType={},IsPandas=1".format(temp_report,n+1))['STYLERATE']
        fund_info = pd.concat([fund_info,temp_info])



