# 2024-06-19 生成 NUC 4因子 每日转债策略篮子


import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote
import numpy as np
import time
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from bs4 import BeautifulSoup
from datetime import date,datetime, timedelta
from chinese_calendar import is_workday
from constants import CB_OF_TODAY_CONV_BIAS,DATA_LOG_FILE,STRATEGY_FILE,STRATEGY_CONV_BIAS
import logging
from chinese_calendar import is_workday
import re
from option_class import OptionValue

#2024-01-21 增加一个输出，用于跟踪crontab
print("CB NUC F4 start at:",datetime.now())

def is_trade_day(ddate):
    if is_workday(ddate):
        if ddate.isoweekday() < 6:
            return True
    return False

# 不排除评级
excludeRatings = []

dt = date.today()

def GetCBData(start_date):                                           #从数据库获取start_date之后的转债数据

    password = quote('Happy$4ever')

    engine = create_engine(f'mysql+mysqlconnector://root:{password}@192.168.8.78:3306/CB_HISTORY')
    conn = engine.connect()

    query = f"SELECT * FROM CB_DATA WHERE trade_date >=  '{start_date}'"

    df = pd.read_sql(query, conn)

    engine.dispose()

    return df

def ExcludeRatings(df):                                 #排除低评级

    #return df[~df.评级.isin(excludeRatings)]
    return df[~df.rating.isin(excludeRatings)]

#2024-01-25 获取强赎计数
def extract_num(s):
    match = re.search(r'至少还需(\d+)', s)
    return int(match.group(1)) if match else 100        #如果没有‘至少还需n天',则返回一个大数100

def ExcludeForcedRedem(df):                             #排除强赎 集思录发布过强赎公告状态!,并且排除备注不强赎

    #return pd.concat([df[~df['is_call'].str.contains('公告提示强赎')], df[~df['is_call'].str.contains('公告实施强赎')],df[~df['is_call'].str.contains('公告到期赎回')],df[~df['is_call'].str.contains('已满足强赎条件')]],axis=0)
    filter_condition = ~df['is_call'].str.contains('公告提示强赎|公告实施强赎|公告到期赎回|已满足强赎条件|已公告强赎|到期')
    rdf = df.loc[filter_condition].copy()

    #rdf = df[~df['is_call'].str.contains('公告提示强赎|公告实施强赎|公告到期赎回|已满足强赎条件|已公告强赎|到期')] # 综合禄得和集思录的强赎标志
    
    rdf.loc[:, 'force_days'] = rdf['is_call'].apply(extract_num)
    rdf = rdf[rdf['force_days'] > 3]                    #只保留强赎计数大于3天的转债

    rdf.to_excel(r'C:\\Temp\\rdf.xlsx')
    return rdf


    #return pd.concat([df[~df['转债名称'].str.contains('!')], df[df['转债名称'].str.contains('!') & df['强赎状态'].str.contains('不强赎')]],axis=0)

def ExcludeST(df):  

    return df[~df.name_stk.str.upper().str.contains('ST')]    
    

#2024-06-19 NUC F4 策略
def get_NUC_F4(date_str):                              #获取15%乖离率的转债
    
    
    date = datetime.strptime(date_str, '%Y-%m-%d').date()
    # 2024-02-21 改为取之前40天的数据,春节长假30天不够导致计算错误
    start_date = date - timedelta(days=40)

    cbdf = GetCBData(start_date.strftime('%Y-%m-%d'))

    # 计算15日乖离率因子

    grouped = cbdf.groupby('code')
    #grouped_sorted = grouped.apply(lambda x: x.sort_values(by='trade_date')).reset_index(drop=True)

    dfnew = pd.DataFrame()
    
    #cbdf['prem_sma'] = grouped_sorted['conv_prem'].shift(1).rolling(window=10).mean().values #reset_index(level=0, drop=True)       #shift(1)： 用昨日的均线
    #grouped 计算最后一日的sma数据错误，只能改用循环：

    for name,df in grouped:                                                         
        df['prem_sma'] = df['conv_prem'].shift(1).rolling(window=15).mean().values
        dfnew = pd.concat([dfnew, df])

    cbdf = dfnew
    
    cbdf['prem_bias'] = cbdf['conv_prem'] - cbdf['prem_sma']
    
    #计算 期权价值因子

    option_value = OptionValue()
    ov_df = option_value.get_allcb_option_value()

    cbdf = pd.merge(cbdf, ov_df, on='code', how='left')

    #保留当日数据
    df = cbdf[cbdf['trade_date'] == date].copy()   
    df.to_excel(r'C:\\Temp\\xxprem.xlsx') 

    #排除强赎和低评级,ST
    df = ExcludeRatings(df)
    df = ExcludeForcedRedem(df)
    df = ExcludeST(df)      

    #排除设置 
    df = df[df['conv_prem']<=0.5].copy()
    df = df[df['remain_cap']<=5].copy()
    df = df[df['close']<=140].copy()
    df = df[df['left_years']>=0.5].copy()
    df = df[df['left_years']<=5].copy()


    # 计算多因子得分 和 排名(score总分越大越好， rank总排名越小越好)
    df['bias_score'] = df['prem_bias'].rank(ascending=False)

    df['score'] = df['bias_score']
    df['rank'] = df['score'].rank(ascending=False) # 按总分从高到低计算排名

    df = df.sort_values('rank', ascending=True)

    return df

#2024-05-12 V2双刀头 溢价率偏离15+涨幅差策略
def GetCBBias15_V2(date_str):                              
       
    date = datetime.strptime(date_str, '%Y-%m-%d').date()
    # 2024-02-21 改为取之前40天的数据,春节长假30天不够导致计算错误
    start_date = date - timedelta(days=40)

    cbdf = GetCBData(start_date.strftime('%Y-%m-%d'))

    #cbdf.to_excel('prem.xlsx')

    grouped = cbdf.groupby('code')
    #grouped_sorted = grouped.apply(lambda x: x.sort_values(by='trade_date')).reset_index(drop=True)

    dfnew = pd.DataFrame()
    
    #cbdf['prem_sma'] = grouped_sorted['conv_prem'].shift(1).rolling(window=10).mean().values #reset_index(level=0, drop=True)       #shift(1)： 用昨日的均线
    #grouped 计算最后一日的sma数据错误，只能改用循环：

    for name,df in grouped:                                                         
        df['prem_sma'] = df['conv_prem'].shift(1).rolling(window=15).mean().values
        
        dfnew = pd.concat([dfnew, df])

    cbdf = dfnew
    
    cbdf['conv_bias15'] = cbdf['conv_prem'] - cbdf['prem_sma']
    cbdf['pct_chg_gap5'] = cbdf['pct_chg_5']-cbdf['pct_chg_5_stk']


    #计算 期权价值因子

    option_value = OptionValue()
    ov_df = option_value.get_allcb_option_value()

    cbdf = pd.merge(cbdf, ov_df, on='code', how='left')
    
    #保留当日数据
    df = cbdf[cbdf['trade_date'] == date].copy()   
    

    #排除强赎和低评级,ST
    df = ExcludeRatings(df)
    df = ExcludeForcedRedem(df)
    df = ExcludeST(df)      

    #排除设置 
    df = df[df['conv_prem']<=0.5].copy()
    df = df[df['remain_cap']<=5].copy()
    df = df[df['close']<=140].copy()
    df = df[df['left_years']>=0.5].copy()
    df = df[df['left_years']<=5].copy()

    # 生成因子字典，name:列名，weight:权重, ascending:排序方向,False为负相关
    rank_factors = [
        {'name': 'conv_bias15', 'weight': 3, 'ascending': False}, 
        {'name': 'pct_chg_gap5', 'weight': 1, 'ascending': False}, ]

    # 计算多因子得分 和 排名(score总分越大越好， rank总排名越小越好)

    for factor in rank_factors:
        if factor['name'] in df.columns:
            df[f'{factor["name"]}_score'] = df[factor["name"]].rank(ascending=factor['ascending']) * factor['weight']
        else:
            print(f'未找到因子【{factor["name"]}】, 跳过')

    df['score'] = df[df.filter(like='score').columns].sum(axis=1, min_count=1)

    df['rank'] = df['score'].rank(ascending=False) # 按总分从高到低计算排名

    df = df.sort_values('rank', ascending=True)

    return df
        
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=DATA_LOG_FILE, level=logging.INFO, format=LOG_FORMAT)

dt = date.today()

'''
if is_trade_day(dt) == False:
    logging.warning('非交易日退出:----->'+str(dt))
    sys.exit(0)
'''
logging.warning('CBofToday 溢价率偏离率:----->'+str(dt))

dt_str = dt.strftime('%Y-%m-%d')

#dt_str = '2024-06-12'
df = GetCBBias15_V2(dt_str)

now = datetime.now()
current_hour = now.hour

if current_hour < 15:

    filename=CB_OF_TODAY_CONV_BIAS+str(dt)+'-V2-IN'+'.xlsx'                         #2023-10-10 盘中选债，收盘前执行
        
else:
    filename=CB_OF_TODAY_CONV_BIAS+str(dt)+'-V2-OUT'+'.xlsx'                          #盘后选债，用于比较


df.to_excel(filename)

filename = STRATEGY_FILE+str(dt)+'.xlsx'                            

print(filename)
df.to_excel(filename, sheet_name=STRATEGY_CONV_BIAS, index=False)               #写入策略篮子文件，用于交易    


