import re
import urllib.request
from datetime import datetime

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


def loadHKExOptions(dt):

    url = 'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsio' +  dt.strftime('%y%m%d')  + '.htm'
    with urllib.request.urlopen(url) as f:
        html = f.read().decode('utf-8')
    print (len(html))
    separator = ', '

    #Extract Header Dates 
    ReportDateStrMatches = re.search('   Business Day([\\s\\S]*?)After-Hours Trading Session', html, re.MULTILINE )
    print(ReportDateStrMatches.group())
    matchdates = tuple(re.findall ('(\\d\\d (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) \\d\\d\\d\\d)', ReportDateStrMatches.group()))
    format_str="%d %b %Y"
    if (len(matchdates)==2):
        ndt = datetime.strptime(matchdates[0][0], format_str)
        dt = datetime.strptime(matchdates[1][0], format_str)
        print(ndt,dt)
    
    matches = re.finditer('MONTH     PRICE([\\s\\S]*?)TOTAL', html)
    matches=[]
    for k in matches:
        match_txt = k.group(0)
        rows = match_txt.split('\n')
        for elem in rows:
            cell = elem.split()
            if len(cell)==22:
                print (separator.join(cell))

#testDBConnection()
loadHKExOptions(datetime(2021, 4, 7))
