import sys
import re
import urllib.request
from datetime import timedelta,datetime

import mysql.connector

mydb = mysql.connector.connect(
  host="francisww.asuscomm.com",
  port=9906,
  user="t4user",
  password="t4user",
  database="world"
)


def testDBConnection():
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM city")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

def loadHKExFutures(dt):

    print('scrap futures:', dt)
    try:
        url = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsif' +  dt.strftime('%y%m%d')  + '.htm'
        with urllib.request.urlopen(url) as f:
            html = f.read().decode('utf-8')
    except:
        return
    
    #print (len(html))
    separator = ', '

    #Extract Header Dates 
    ReportDateStrMatches = re.search('   Business Day([\\s\\S]*?)After-Hours Trading Session', html, re.MULTILINE )
    #print(ReportDateStrMatches.group())
    matchdates = tuple(re.findall ('(\\d\\d (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) \\d\\d\\d\\d)', ReportDateStrMatches.group()))
    format_str="%d %b %Y"
    if (len(matchdates)==2):
        ndt = datetime.strptime(matchdates[0][0], format_str)
        dt = datetime.strptime(matchdates[1][0], format_str)
        #print(ndt,dt)
    #input("Press Enter to continue...")
    
    #Extract Future Tables
    matches = re.finditer('Month     Price([\\s\\S]*?)All Contracts Total', html)
    #matches=[]
    for k in matches:
        match_txt = k.group(0)
        rows = match_txt.split('\n')
        mycursor = mydb.cursor()
        for elem in rows:
            cell = elem.split()

            replacements = [
                (',', ''),
                ('^-$', '0')
            ]

            #cleaned = [x.replace(',','') for x in cell] #Remove comma in each of the cells and single '-' field
            #cant figure a way to do fluent single - char replace to 0 then do it cell by cell using batch replace with re
            cleaned=[]
            for x in cell:
                for old, new in replacements:
                    x = re.sub(old, new, x)
                cleaned.append(x)

            if len(cleaned)==19:
                #APR-21   28,811  28,999  28,794  28,931  12,523  |  29,130  29,148  28,516 108,160   28,541     -276  |   29,742   27,470  120,683   100,427     +3,688
                #print (separator.join(cleaned))
                contractMonth = datetime.strptime(cleaned[0], '%b-%y')
                rptdt = dt # from parameter
                underly = "HSI"
                ndt = ndt # from header
                no = cleaned[1]
                nh = cleaned[2]
                nl = cleaned[3]
                nc = cleaned[4]
                nv = cleaned[5]
                o = cleaned[7]
                h = cleaned[8]
                l = cleaned[9]
                v = cleaned[10]
                c = cleaned[11]
                cc = cleaned[12]
                hhv = cleaned[14]
                llv = cleaned[15]
                cv = cleaned[16] #combined vol
                oi = cleaned[17]
                oic = cleaned[18]
    
                try:
                    mycursor.execute("""
                        REPLACE INTO hkex_futures_contract (
                            rptdt,ContractMonth,underly,ndt,no,nh,nl,nc,nv,dt,o,h,l,c,v,cc,hhv,llv,cv,oi,oic)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (rptdt,contractMonth,underly,ndt,no,nh,nl,nc,nv,dt,o,h,l,c,v,cc,hhv,llv,cv,oi,oic)
                    )
                except:
                    print(rptdt,contractMonth,underly,ndt,no,nh,nl,nc,nv,dt,o,h,l,c,v,cc,hhv,llv,cv,oi,oic)
                    sys.exit()

        mydb.commit() #Commit per report to lower overhead




def loadHKExOptions(dt):

    print(dt)
    try:
        url = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsio' +  dt.strftime('%y%m%d')  + '.htm'
        with urllib.request.urlopen(url) as f:
            html = f.read().decode('utf-8')
        #print (len(html))
    except:
        return

    separator = ', '

    #Extract Header Dates 
    ReportDateStrMatches = re.search('   Business Day([\\s\\S]*?)After-Hours Trading Session', html, re.MULTILINE )
    print(ReportDateStrMatches.group())
    matchdates = tuple(re.findall ('(\\d\\d (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) \\d\\d\\d\\d)', ReportDateStrMatches.group()))
    format_str="%d %b %Y"
    if (len(matchdates)==2):
        ndt = datetime.strptime(matchdates[0][0], format_str)
        dt = datetime.strptime(matchdates[1][0], format_str)
        #print(ndt,dt)
    #input("Press Enter to continue...")
    
    #Extract Future Tables
    matches = re.finditer('MONTH     PRICE([\\s\\S]*?)TOTAL', html)
    #matches=[]
    for k in matches:
        match_txt = k.group(0)
        rows = match_txt.split('\n')
        for elem in rows:
            cell = elem.split()
            if len(cell)==22:
                #DEC-25, 37200, P, 0, 0, 0, 0, 0, |, 0, 0, 0, 12035, +156, 17, 0, |, 0, 0, 0, 0, 0
                print (separator.join(cell))
                contractMonth = cell[0]
                rptdt = dt # from parameter
                underly = "HSI"
                ndt = ndt # from header



#testDBConnection()  #POC for Mysql

dt = datetime(2019,1,1)
while dt <= datetime.today():
    if dt.weekday() < 5:  # Sat/Sun = 5,6
        loadHKExFutures(dt)
    dt = dt + timedelta(days=1)
    
#loadHKExOptions(datetime(2021, 4, 7))
