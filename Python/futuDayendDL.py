import time
import ctypes
import os
import sys
import re
import urllib.request
from datetime import timedelta,datetime
import mysql.connector
from futu import *

#mysql endpoint
mydb = mysql.connector.connect(
  host="francisww.asuscomm.com",
  port=9906,
  user="t4user",
  password="t4user",
  database="world"
)

#futu connection endpoint
HOST='192.168.1.128'
PORT=11111

def getHKStockList():
    quote_ctx = OpenQuoteContext(host=HOST, port=PORT)
    

    ret, data = quote_ctx.get_stock_basicinfo(Market.HK, SecurityType.STOCK)
    if ret == RET_OK:
        stocks=data['code'].values.tolist()
        print ("stock : " + str(len(stocks)))
        #print(data[data['code'].str.startswith('HK.07')]['code'].values.tolist())
    else:
        print('error:', data)
    
    ret, data = quote_ctx.get_stock_basicinfo(Market.HK, SecurityType.ETF)
    if ret == RET_OK:
        etf=data[data['code'].str.startswith('HK.07')]['code'].values.tolist()
        #print(data[data['code'].str.startswith('HK.07')]['code'].values.tolist())
        print ("etf : " + str(len(etf)))
        stocks.extend(etf)
        #print(etf)
    else:
        print('error:', data)
    

    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
    #return list(set(stocks)).sort()
    stocks.append("HK.800000")
    stocks.append("HK.800125")
    stocks.append("HK_FUTURE.999010")
    stocks.append("HK_FUTURE.999011")
    return stocks

def LoadFutuDayendDAYPrice(stocks, fr, to):
    batchsize=200
    mode='D'
    cnt = len(stocks)
    i=0
    j=0
    b=0
    codes=[]
    while (j<batchsize and i<cnt) or mode=='P':

        codes.append(stocks[i])
        j=j+1
        i=i+1
        
        if j==batchsize or i==cnt:
        
            quote_ctx = OpenQuoteContext(host='192.168.1.128', port=11111)
            #ret_sub, err_message = quote_ctx.subscribe(codes, [SubType.K_DAY], subscribe_push=False)
            # 先订阅K 线类型。订阅成功后FutuOpenD将持续收到服务器的推送，False代表暂时不需要推送给脚本


            print( 'doing batch' + str(b) + ' with ' + str(batchsize) +  ' elements for cnt=' +str(cnt))
            for code in codes:
                ret_sub, err_message = quote_ctx.subscribe([code], [SubType.K_DAY], subscribe_push=False)
                if ret_sub == RET_OK:  # 订阅成功
                    ret, data = quote_ctx.get_cur_kline(code, 1000, SubType.K_DAY, AuType.QFQ)  # 获取港股00700最近2个K线数据
                    if ret == RET_OK:
                        #print(data)
                        filter_mask = data['time_key'] > fr  + ' 16:30:00'
                        filter_mask1 = data['time_key'] <= to  + ' 16:30:00'
                        filtered_df = data[filter_mask & filter_mask1]
                        
                        try:
                        
                            print(filtered_df.iloc[[0,-1]])            
                            #filtered_df.to_csv(code+'d.csv')
                            #data.to_csv(code+'n.csv')
                        except Exception:
                            pass
                    
                        #print(data['turnover_rate'][0])   # 取第一条的换手率
                        #print(data['turnover_rate'].values.tolist())   # 转为list
                    else:
                        print('error:', data)
                else:
                    print('subscription failed', err_message)
                    
                quote_ctx.unsubscribe([code], [SubType.K_DAY])
            
            print( 'Done batch' + str(b) + ' with ' + str(j) +  ' elements')
            quote_ctx.close()  # 关闭当条连接，FutuOpenD会在1分钟后自动取消相应股票相应类型的订阅              
            codes=[]
            j=0
            b=b+1
            #if i!=cnt:
            print ('sleep 60 secs')
            time.sleep(62)
            
            
            if i==cnt and mode=='P':
                i=0
                j=0
                b=0
                codes=[]
        if i==cnt:        
            break


def SaveFutuHistDayToSQL():
    directory = 'Z:/alldaydata2'
    
    cursor=mydb.cursor()
    for file in os.listdir(directory):
        #filename = os.fsdecode(file)
        filename = file
        if filename.endswith(".csv") : 
            fn = os.path.join(directory, filename)
            print(fn)
            df = pd.read_csv (fn)
            #print(df)
            my_data = []
            sql = "INSERT INTO futudayprice (code,dt,o,h,l,c,pe,turnoverrate,v,turnover) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for i,row in df.iterrows():

                code=row[1]
                dt=datetime.strptime(str(row[2]),'%Y-%m-%d %H:%M:%S')
                o=round(row[3],3)
                c=round(row[4],3)
                h=round(row[5],3)
                l=round(row[6],3)
                pe=round(row[7],3)
                turnoverrate=round(row[8],3)
                volume=round(row[9],3)
                turnover=round(row[10],3)
                val = (code,dt,o,h,l,c,pe,turnoverrate,volume,turnover)
                #print (val)

                try:                    
                    my_data.append(val)
                    i=i+1

                except:
                    print(sql)
                    sys.exit()
                # if i==10:
                #     break
                if i%10000==0:
                    #mydb.commit()
                    #Use Execute Many to save time
                    cursor.executemany(sql, my_data)
                    print(file + ' batch inserted ' + str(i) + ' rows')
                    mydb.commit()
                    my_data = []

            cursor.executemany(sql, my_data)
            print(file + ' batch inserted ' + str(i) + ' rows')
            mydb.commit()

            continue
        else:
            continue

stocks = getHKStockList()
print ("total : " + str(len(stocks)))
print(stocks[-10:])

#LoadFutuDayendDAYPrice(stocks, '2010-01-01', '2012-12-31')

SaveFutuHistDayToSQL()
