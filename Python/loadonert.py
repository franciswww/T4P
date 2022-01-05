import time
import ctypes
import os
import sys

#DAYEND DL for stocks

from futu import *
#quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

fr='2022-01-04'
to='2022-01-05'

from futu import *

#filename = sys.argv[3]
#filename = sys.argv[3]
batchsize= 200
mode = "D"  # D:Dayend P: Probe
stocks = []
#stocks.append('HK.800125')
stocks.append('HK_FUTURE.999010')
stocks.append('HK.HSI220107C23400')
stocks.append('HK.HSI220107P23200')
stocks.append('HK.HSI220107C23600')
stocks.append('HK.HSI220107P23000')
stocks.append('HK.HSI220128C23400')
stocks.append('HK.HSI220128P23200')
stocks.append('HK.HSI220128C23600')
stocks.append('HK.HSI220128P23000')
stockcnt=len(stocks)

from futu import *



#stocks.append(etf)

print(stocks)
print('Total Stocks: ' + str(stockcnt))
#input("Press Enter to continue...")
cnt = len(stocks)
i=0
j=0
b=0
codes=[]

dict = {}
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
            ret_sub, err_message = quote_ctx.subscribe([code], [SubType.K_5M], subscribe_push=False)
            if ret_sub == RET_OK:  # 订阅成功
                ret, data = quote_ctx.get_cur_kline(code, 1000, SubType.K_5M, AuType.QFQ)  # 获取港股00700最近2个K线数据
                if ret == RET_OK:
                    #print(data)
                    filter_mask = data['time_key'] > fr  + ' 16:30:00'
                    filter_mask1 = data['time_key'] <= to  + ' 16:30:00'
                    filtered_df = data[filter_mask & filter_mask1]
                    
                    try:
                        dict[code] = filtered_df                    
                        print(filtered_df.iloc[[0,-1]])            
                        filtered_df.to_csv(code+'d.csv')
                        #data.to_csv(code+'n.csv')

                    except Exception:
                        pass
                
                    #print(data['turnover_rate'][0])   # 取第一条的换手率
                    #print(data['turnover_rate'].values.tolist())   # 转为list
                else:
                    print('error:', data)
            else:
                print('subscription failed', err_message)
                
            quote_ctx.unsubscribe([code], [SubType.K_5M])
        
        print( 'Done batch' + str(b) + ' with ' + str(j) +  ' elements')
        quote_ctx.close()  # 关闭当条连接，FutuOpenD会在1分钟后自动取消相应股票相应类型的订阅              
        codes=[]
        j=0
        b=b+1
        #if i!=cnt:
        #print ('sleep 60 secs')
        #time.sleep(62)
        
        
        if i==cnt and mode=='P':
            i=0
            j=0
            b=0
            codes=[]
    if i==cnt:        
        break
print('Done 0')
#print(dict["HK.HSI220107C23400"])      

#result = pd.concat([dict["HK_FUTURE.999010"], dict["HK.HSI220107C23400"], dict["HK.HSI220107P23200"]], axis=1)

result = dict["HK_FUTURE.999010"]
result["W240C"] = dict["HK.HSI220107C23400"]["close"]
result["W220P"] = dict["HK.HSI220107P23200"]["close"]
result["W260C"] = dict["HK.HSI220107C23600"]["close"]
result["W000P"] = dict["HK.HSI220107P23000"]["close"]
result["M240C"] = dict["HK.HSI220128C23400"]["close"]
result["M220P"] = dict["HK.HSI220128P23200"]["close"]
result["M260C"] = dict["HK.HSI220128C23600"]["close"]
result["M200P"] = dict["HK.HSI220128P23000"]["close"]
result["WS200"] = result["W240C"] + result["W220P"]
result["WS400"] = result["W260C"] + result["W000P"]
result["MS200"] = result["M240C"] + result["M220P"]
result["MS400"] = result["M260C"] + result["M200P"]

print(result)
result.to_csv('combos.csv')
#loop_forever = True
#while loop_forever:
#    try:
#        time.sleep(60)
#    except KeyboardInterrupt:
#        loop_forever = False

print("Done") # StockQuoteTest自己的处理逻辑
        
