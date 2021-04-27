import re
import urllib.request
with urllib.request.urlopen('https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsio210426.htm') as f:
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