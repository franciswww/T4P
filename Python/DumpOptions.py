import time
import ctypes
import os
import sys

#DAYEND DL for stocks

from futu import *
#quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

fr='2022-01-25'
to='2022-01-26'

from futu import *

#filename = sys.argv[3]
#filename = sys.argv[3]
batchsize= 200
mode = "D"  # D:Dayend P: Probe
stocks = []
#stocks.append('HK.800125')
stocks.append('HK_FUTURE.999010')


for i in range(23000,25200,200):
    stocks.append('HK.HSI220128C' + str(i) + "000") 

for i in range(23000,25200,200):
        stocks.append('HK.HSI220128P' + str(i) + "000") 

#for i in range(20000,26000,200):
#    stocks.append('HK.HSI220114C' + str(i))

#for i in range(20000,26000,200):
#    stocks.append('HK.HSI220114P' + str(i))

#for i in range(20000,26000,200):
#    stocks.append('HK.HSI220128C' + str(i))

#for i in range(20000,26000,200):
#    stocks.append('HK.HSI220128P' + str(i))


stocks.append('HK.HSI220128C24400000')
stocks.append('HK.HSI220128P24200000')
stocks.append('HK.HSI220128C24600000')
stocks.append('HK.HSI220128P24000000')
stocks.append('HK.HSI220128C24400000')
stocks.append('HK.HSI220128P24200000')
stocks.append('HK.HSI220128C24600000')
stocks.append('HK.HSI220128P24000000')
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
    
        quote_ctx = OpenQuoteContext(host='francisww.asuscomm.com', port=11111)
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
                        filtered_df.to_csv(code+ '_' + to + '.csv')
                        #data.to_csv(code+'.csv')

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


exit

result = dict["HK_FUTURE.999010"]
result["W238P"] = dict["HK.HSI220128P23800000"]["close"]
result["W240C"] = dict["HK.HSI220128C24000000"]["close"]
result["W240P"] = dict["HK.HSI220128P24000000"]["close"]
result["W242C"] = dict["HK.HSI220128C24200000"]["close"]
result["W242P"] = dict["HK.HSI220128P24200000"]["close"]
result["W244C"] = dict["HK.HSI220128C24400000"]["close"]
result["W244P"] = dict["HK.HSI220128P24400000"]["close"]
result["W246C"] = dict["HK.HSI220128C24600000"]["close"]
result["W246P"] = dict["HK.HSI220128P24600000"]["close"]
result["W248C"] = dict["HK.HSI220128C24800000"]["close"]
result["W248P"] = dict["HK.HSI220128P24800000"]["close"]
result["W250C"] = dict["HK.HSI220128C25000000"]["close"]
result["W250P"] = dict["HK.HSI220128P25000000"]["close"]


result["M242P"] = dict["HK.HSI220128P24200000"]["close"]
result["M244C"] = dict["HK.HSI220128C24400000"]["close"]
result["M244P"] = dict["HK.HSI220128P24200000"]["close"]
result["M246C"] = dict["HK.HSI220128C24600000"]["close"]
result["M240P"] = dict["HK.HSI220128P24000000"]["close"]


result["W248CW240P"] = result["W248C"] + result["W240P"]
result["W246CW242P"] = result["W246C"] + result["W242P"]


result["W240CW240P"] = result["W240C"] + result["W240P"]
result["W242CW242P"] = result["W242C"] + result["W242P"]
result["W244CW244P"] = result["W244C"] + result["W244P"]
result["W246CW246P"] = result["W246C"] + result["W246P"]
result["W248CW248P"] = result["W248C"] + result["W248P"]
result["W250CW250P"] = result["W248C"] + result["W248P"]

result["W242CW240P"] = result["W242C"] + result["W240P"]
result["W244CW242P"] = result["W244C"] + result["W242P"]
result["W246CW244P"] = result["W246C"] + result["W244P"]
result["W248CW246P"] = result["W248C"] + result["W246P"]
result["W250CW248P"] = result["W250C"] + result["W248P"]


result["W244CW240P"] = result["W244C"] + result["W240P"]
result["W246CW242P"] = result["W246C"] + result["W242P"]
result["W248CW244P"] = result["W248C"] + result["W244P"]
result["W250CW246P"] = result["W250C"] + result["W246P"]





result["MS200"] = result["M244C"] + result["M242P"]
result["MS400"] = result["M246C"] + result["M240P"]

print(result)
result.to_csv('combos220128.csv')
#loop_forever = True
#while loop_forever:
#    try:
#        time.sleep(60)
#    except KeyboardInterrupt:
#        loop_forever = False

print("Done") # StockQuoteTest自己的处理逻辑
        
