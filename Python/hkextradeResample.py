import sys
import glob
import re
import urllib.request
from datetime import timedelta,datetime
import pandas as pd
import mysql.connector
import math
import zipfile

mydb = mysql.connector.connect(
  host="francisww.asuscomm.com",
  port=9906,
  user="t4user",
  password="t4user",
  database="world"
)

# import sqlalchemy
# database_username = 'ENTER USERNAME'
# database_password = 'ENTER USERNAME PASSWORD'
# database_ip       = 'ENTER DATABASE IP'
# database_name     = 'ENTER DATABASE NAME'
# database_connection = sa.create_engine('mysql+pyodbc://{0}:{1}@{2}/{3}'.
#                                                format('t4user', 't4user', 
#                                                       '192.168.1.128', 'world'))

from pandas.io import sql
#import MySQLdb                                                      
import numpy as np

def xstr(s):
    if s is None:
        return ''
    return str(s)

def agg_ohlcv(x):
    arr = x['price'].values
    names = {
        'low': min(arr) if len(arr) > 0 else np.nan,
        'high': max(arr) if len(arr) > 0 else np.nan,
        'open': arr[0] if len(arr) > 0 else np.nan,
        'close': arr[-1] if len(arr) > 0 else np.nan,
        'volume': sum(x['quantity'].values) if len(x['quantity'].values) > 0 else 0,
    }
    return pd.Series(names)

basepath = 'Z:\\hkexdata\\'
months = ['202007']
files = ['_01_TR_AHT.csv' , '_01_TR.csv']

#my_conn=create_engine("mysql+mysqldb://t4user:t4user@francisww.asuscomm.com:9906/world")
cursor=mydb.cursor()
cursor.fast_executemany = True


def ProcessHKExFiles():
    for mth in months:

        dict = {}
        path  = basepath + '\\' + mth + '\\'
        for f in files:
            for file in glob.glob(path + mth  + '*' + f):
                print(file)
                table = 'temp'+mth
                #print ('tablename:' + table)
                df = pd.read_csv(file, dtype=str, header=None)
                #df.rename(columns={"0": "code", "1": "type", "2": "contract", "3":"strike", "4":"cp", "5":"dt", "6":"time","7":"price","8":"filler1","9":"filler2"}, errors="raise")
                #print(df.dtypes)
                df.columns=["code", "type", "contract", "strike", "cp", "date", "time", "price", "quantity", "trade_type"]
                df.loc[:,'datetime'] = pd.to_datetime(df.date.astype(str)+' '+df.time.astype(str))
                df["price"] = df.price.astype(float)
                df["quantity"] = df.quantity.astype(float)
                #df["strike"] = df.quantity.astype(int)
                df = df.set_index('datetime')

                futures = df[(df.code=="HSI") & (df.type =="F")]
                options = df[(df.code=='HSI') & (df.type=="O")]
                #futures.loc[:,'datetime'] = pd.to_datetime(futures.date.astype(str)+' '+futures.time.astype(str))
                #options.loc[:,'datetime'] = pd.to_datetime(options.date.astype(str)+' '+options.time.astype(str))
                #futures["price"] = futures.price.astype(float)
                #futures["quantity"] = futures.quantity.astype(float)
                #futures["strike"] = futures.quantity.astype(int)
                #futures = futures.set_index('datetime')
                #options = options.set_index('datetime')
                if "F" not in dict:
                    dict["F"]  = futures
                else:
                    dict["F"] = dict["F"].append(futures)
                if "O" not in dict:
                    dict["O"]  = options
                else:
                    dict["O"] = dict["O"].append(options)
        #print(dict["F"])
        #print(dict["O"])
        
        GroupF = dict["F"].groupby(['contract']).size().reset_index().rename(columns={0:'count'})
        print(type(GroupF))
        print(GroupF)

        GroupO = dict["O"].groupby(['contract', 'cp', 'strike']).size().reset_index().rename(columns={0:'count'})
        print(GroupO)
        print(GroupO.dtypes)

        #spot = futures[futures.contract=="20200828"]
        #print(spot)
        #resampled = spot["price"].resample('5Min').ohlc()
        #resampled.to_csv('resampled.csv')

        fresampled = None
        regular=None

        for i,row in GroupF.iterrows():
            contract = row[0]
            target = dict["F"]
            thistarget = target[target.contract == contract] 
            resampled = thistarget["price"].resample('5Min').ohlc()
            
            print("Write " + str(mth) + "_" + str(contract) + '_sample.csv')
            #resampled.to_csv(str(mth) + "_" + str(contract) + '_sample.csv')
            if (mth == contract[0:6]):
                fresampled = resampled
                regular = contract

        for i,row in GroupO.iterrows():
            contract = row[0]
            cp = row[1]
            strike = row[2]
            target = dict["O"]
            thistarget = target[(target.contract == contract) & (target.cp  == cp) & (target.strike == strike)]
            resampled = thistarget["price"].resample('5Min').ohlc()
            print("Write " + str(mth) + "_" + str(contract) + "_" + str(strike) + cp + '_sample.csv')
            #resampled.to_csv(str(mth) + "_" + str(contract) + "_" + str(strike) + cp + '_sample.csv')       

            #if (contract!=regular & mth == contract[0:6]):
            #    #create column

def Fillna(data_frame):
    data_frame['quantity'] = data_frame['quantity'].fillna(0.0)  # volume should always be 0 (if there were no trades in this interval)
    data_frame['close'] = data_frame.fillna(method='pad')  # ie pull the last close into this close
    # now copy the close that was pulled down from the last timestep into this row, across into o/h/l
    data_frame['open'] = data_frame['open'].fillna(data_frame['close']) 
    data_frame['low'] = data_frame['low'].fillna(data_frame['close'])
    data_frame['high'] = data_frame['high'].fillna(data_frame['close'])

def GetOrInsertContract( type, underly, contract, strike, cp):

    if (len(contract)==4):
        contract = "20" + contract + "01"

    mycursor = mydb.cursor()
    sql = """SELECT id FROM `hkex_contract_spec` WHERE type = '%s' and underly = '%s' and contractmonth = '%s' and strike = '%s' and cp = '%s'""" % (type, underly, contract, strike, cp)
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    #if (myresult[0][0] != None):
    if (len(myresult) != 0):
        return myresult[0][0]
    else:
        val = (type,contract,underly,strike,cp)
        sql = "INSERT INTO `hkex_contract_spec` (type,contractMonth,underly,strike,cp) VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(sql, val)
        id = mycursor.lastrowid
        mydb.commit()
        return id  

def ProcessHKExFilesZip():

    for file in glob.glob(basepath + 'tt_D01_??????*.zip'):

        dict = {}
        mth = file[-10:-4]
        zip = zipfile.ZipFile(file)
        #print (zip.namelist())
        
        #if mth < '201904':
        #    continue

        print(mth)
                
        
        for file in files:
            fn = mth + file
            print(fn)
            df = pd.read_csv(zip.open(fn), dtype=str, header=None)
            #df.rename(columns={"0": "code", "1": "type", "2": "contract", "3":"strike", "4":"cp", "5":"dt", "6":"time","7":"price","8":"filler1","9":"filler2"}, errors="raise")
            #print(df.dtypes)
            df.columns=["code", "type", "contract", "strike", "cp", "date", "time", "price", "quantity", "trade_type"]
            df.loc[:,'datetime'] = pd.to_datetime(df.date.astype(str)+' '+df.time.astype(str))
            df["price"] = df.price.astype(float)
            df["quantity"] = df.quantity.astype(float)
            #df["strike"] = df.quantity.astype(int)
            df = df.set_index('datetime')
            mask = (df['contract'].str.len() == 4)
            print(df)
            if len(df[mask].index) != 0:
                df['contract'] = '20'+df[mask]['contract']+'01'
            print(df)
            futures = df[(df.code=="HSI") & (df.type =="F")]
            options = df[(df.code=='HSI') & (df.type=="O")]
            #futures.loc[:,'datetime'] = pd.to_datetime(futures.date.astype(str)+' '+futures.time.astype(str))
            #options.loc[:,'datetime'] = pd.to_datetime(options.date.astype(str)+' '+options.time.astype(str))
            #futures["price"] = futures.price.astype(float)
            #futures["quantity"] = futures.quantity.astype(float)
            #futures["strike"] = futures.quantity.astype(int)
            #futures = futures.set_index('datetime')
            #options = options.set_index('datetime')
            if "F" not in dict:
                dict["F"]  = futures
            else:
                dict["F"] = dict["F"].append(futures)
            if "O" not in dict:
                dict["O"]  = options
            else:
                dict["O"] = dict["O"].append(options)

        #print(dict["F"])
        #print(dict["O"])
        
        GroupF = dict["F"].groupby(['contract']).size().reset_index().rename(columns={0:'count'})
        print(type(GroupF))
        print(GroupF)

        GroupO = dict["O"].groupby(['contract', 'cp', 'strike']).size().reset_index().rename(columns={0:'count'})
        print(GroupO)
        print(GroupO.dtypes)

        #spot = futures[futures.contract=="20200828"]
        #print(spot)
        #resampled = spot["price"].resample('5Min').ohlc()
        #resampled.to_csv('resampled.csv')

        fresampled = None
        regular=None
        for i,row in GroupF.iterrows():
            contract = row[0]
            target = dict["F"]
            thistarget = target[target.contract == contract]
            resampled = thistarget["price"].resample('5Min').ohlc()            
            vol = thistarget["quantity"].resample('5Min').sum()
            combined = pd.concat([resampled, vol], axis=1)
            Fillna(combined)
            combined.index = combined.index + timedelta(minutes=5)
            #print(combined)
            combined = combined[(combined.index.strftime('%H:%M') >= '09:15') & (combined.index.strftime('%H:%M') <= '12:00') | (combined.index.strftime('%H:%M') >= '13:00') & (combined.index.strftime('%H:%M') <= '16:30') | (combined.index.strftime('%H:%M') >= '17:15') | (combined.index.strftime('%H:%M') <= '03:00')  ]
            
            print("Write " + str(mth) + "_" + str(contract) + '_sample.csv')
            #combined.to_csv(str(mth) + "_" + str(contract) + '_sample.csv')
            contractid = GetOrInsertContract( 'F', 'HSI', contract, 0, '')
            sql = "INSERT INTO hkex_futures_contract_rt5 (contractid,dt,o,h,l,c,v) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            my_data = []

            i=0
            for idx,row in combined.iterrows():      
                i=i+1          
                underly = 'HSI'
                #dt=datetime.strptime(str(row['datetime']),'%Y-%m-%d %H:%M:%S')
                dt=idx
                o=round(row['open'],3)
                c=round(row['close'],3)
                h=round(row['high'],3)
                l=round(row['low'],3)
                v=round(row['quantity'],3)                
                val = (contractid,dt,o,h,l,c,v)
                #print(val)
                my_data.append(val)
                if i%50000==0:
                    cursor.executemany(sql, my_data)
                    print( ' batch inserted ' + str(i) + ' rows')
                    mydb.commit()
                    my_data = []

            
            cursor.executemany(sql, my_data)
            print(' batch inserted ' + str(i) + ' rows')
            mydb.commit()
            my_data = []

            if (mth == contract[0:6]):
                fresampled = combined
                regular = contract

        j=0
        my_data = []
        for i,row in GroupO.iterrows():
            contract = row[0]
            cp = row[1]
            strike = row[2]
            target = dict["O"]
            thistarget= target[(target.contract == contract) & (target.cp  == cp) & (target.strike == strike)]
            resampled = thistarget["price"].resample('5Min').ohlc()
            vol = thistarget["quantity"].resample('5Min').sum()
            combined = pd.concat([resampled, vol], axis=1)
            Fillna(combined)
            combined.index = combined.index + timedelta(minutes=5)
            combined = combined[(combined.quantity != 0)]
            combined = combined[(combined.index.strftime('%H:%M') >= '09:15') & (combined.index.strftime('%H:%M') <= '12:00') | (combined.index.strftime('%H:%M') >= '13:00') & (combined.index.strftime('%H:%M') <= '16:30') | (combined.index.strftime('%H:%M') >= '17:15') | (combined.index.strftime('%H:%M') <= '03:00')  ]
            
            print("Write " + str(mth) + "_" + str(contract) + "_" + str(strike) + cp + '_sample.csv')
            #combined.to_csv(str(mth) + "_" + str(contract) + "_" + str(strike) + cp + '_sample.csv')       

            contractid = GetOrInsertContract( 'O', 'HSI', contract, strike, cp)
            sql = "INSERT INTO hkex_options_contract_rt5 (contractid,dt,o,h,l,c,v) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            
       
            k=0
            for idx,row in combined.iterrows():                
                j=j+1
                
                underly = 'HSI'
                dt=idx
                o=round(row['open'],3)
                c=round(row['close'],3)
                h=round(row['high'],3)
                l=round(row['low'],3)
                v=round(row['quantity'],3)                
                val = (contractid,dt,o,h,l,c,v)
                #print(val)
                my_data.append(val)
                k=k+1
                if j%50000==0:
                    cursor.executemany(sql, my_data)
                    print( ' batch inserted ' + str(j) + ' rows')
                    mydb.commit()
                    my_data = []
            print(' apppend to Insert buffer ' + str(k) + ' rows')

        cursor.executemany(sql, my_data)
        print(' batch inserted ' + str(j) + ' rows')
        mydb.commit()
        my_data = []


        #if (contract!=regular & mth == contract[0:6]):
        #    #create column


ProcessHKExFilesZip()