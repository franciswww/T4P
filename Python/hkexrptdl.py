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
    matches = re.finditer('MONTH     PRICE([\\s\\S]*?)TOTAL', html)
    for k in matches:
        match_txt = k.group(0)
        rows = match_txt.split('\n')
        for elem in rows:
            cell = elem.split()
            if len(cell)==22:
                print (separator.join(cell))

testDBConnection()
loadHKExOptions(datetime(2021, 4, 27))