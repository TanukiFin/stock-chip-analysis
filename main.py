# import 連接本地
"""
    1. 讀取歷史的大戶持股比csv
        * df_whale
        * shareCapital
    2. 下載最新一周的股權分散表csv
        * shareholdings
    3. 計算本周的大戶持股比
    4. 產生本周的picklist
    5. 更新歷史的大戶持股比csv(添加本周)

"""
import numpy as np
import pandas as pd
import requests
import datetime
import ssl

# 1. read csv
df_whale = pd.read_csv("data/whale_ratio.csv")
df_whale.index = pd.to_datetime(df_whale["Date"])
df_whale = df_whale.drop(["Date"], axis=1)
shareCapital = pd.read_csv("data/shareCapital.csv")
shareCapital["公司代號"] = shareCapital["公司代號"].astype(str)
symbolList = shareCapital["公司代號"].tolist()

# 2. 下載最新一周的股權分散表csv
ssl._create_default_https_context = ssl._create_unverified_context
shareholdings = pd.read_csv("https://opendata.tdcc.com.tw/getOD.ashx?id=1-5")
shareholdings["占集保庫存數比例%"] = shareholdings["占集保庫存數比例%"].astype(float)
shareholdings["持股分級"] = shareholdings["持股分級"].astype(int)

# 3. 計算本周的大戶持股比
whaleList = []
level = []
date = pd.to_datetime(shareholdings["資料日期"].iloc[0], format="%Y%m%d")

if date != df_whale.index[-1]:  # 如果有新資料，則開始計算大戶持股比例whale  
    print( "updating latest data: " + str(date)[0:10] +"..." )
    for symbol in symbolList:
        sc = shareCapital[shareCapital["公司代號"]==symbol]["股本"].iloc[0]
        grade = shareholdings[shareholdings["證券代號"]==symbol]["持股分級"]
        if sc < 10*(10**5):                                                            # 第五級: <10億， 1~9級=散戶
            filt = (grade>=10) & (grade<=15)
            level.append(5)
        elif sc >= 10 * (10**5) and sc < 20 * (10**5):                                 # 第四級: 10-20億， 1~10級=散戶
            filt = (grade>=11) & (grade<=15)
            level.append(4)
        elif sc >= 20 * (10**5) and sc < 50 * (10**5):                                 # 第三級: 20-50億， 1~11級=散戶
            filt = (grade>=12) & (grade<=15)
            level.append(3)
        elif sc >= 50 * (10**5) and sc < 100 * (10**5):                                # 第二級: 50-100億， 1~12級=散戶
            filt = (grade>=13) & (grade<=15)
            level.append(2)
        elif sc >= 100 * (10**5):                                                      # 第一級: >100億， 1~13級=散戶
            filt = (grade>=14) & (grade<=15)
            level.append(1)
        else:
            print("wrong")
        try:
            whaleRatio = shareholdings[shareholdings["證券代號"]==symbol][filt]["占集保庫存數比例%"].sum()
            whaleList.append(whaleRatio)
        except:
            print(symbol)
    df = pd.DataFrame([whaleList], columns=symbolList, index=[date])
    # 合併新資料
    df_all = pd.concat([df_whale, df],axis=0)
    df_all.index.name = "Date"
    date2y = pd.to_datetime(date)-datetime.timedelta(days=365*2)
    upNum = df_all.iloc[-1] - df_all.loc[date2y:date].min()
    lastweek = df_all.iloc[-2] - df_all.loc[date2y:date].min()       # 上週 
    
    # 4. 產生本周的picklist
    picklist1 = upNum[ (upNum >= 10) ]  # 只要大於10
    picklist2 = lastweek[ (upNum >= 10) & (lastweek < 10) ]  # 非連續
    picklist = pd.DataFrame([picklist1, picklist2], columns=[date,"上週未大於10"])

    picklist.to_csv("data\\picklist_" + date.strftime("%Y%m%d") + ".csv", index=True, encoding = 'utf-8-sig')
    # 5. 更新歷史的大戶持股比csv(添加本周)
    df_all.to_csv("data\\whale_ratio.csv", index=True, encoding ='utf-8-sig')
    print( "update completed: " + date.strftime("%Y%m%d") )
    print( "updated csv: whale_ratio.csv" )
    print( "created csv: picklist_" + date.strftime("%Y%m%d") )
else: 
    print("no new data, last date: " + date.strftime("%Y%m%d") )