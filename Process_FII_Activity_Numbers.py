##Amit - purpose - Scrap the screener based on the results dates csv file mentioned below.
# Get all data for Quarter_date and Financial_ratio tables, update Final rating etc.

import urllib2
from bs4 import BeautifulSoup
import EmailUtil


class Process_FII_Activity_Numbers:

    def __init__(self):
        self.all_good_flag = True

# ----------------------------------------------------------------------
thisObj = Process_FII_Activity_Numbers()

url = "http://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"

page = urllib2.urlopen(url).read()
soup = BeautifulSoup(page)
#soup.prettify()

div = soup.find("div",{"class":"fidi_tbl CTR"})
table1 = div.find("table")

#EmailUtil.send_email("FII Numbers", str(table1), "")

### SGX Nifty
url = "http://sgxnifty.org/"

page = urllib2.urlopen(url).read()
soup = BeautifulSoup(page)


sgx_table = soup.find("table",{"class":"indexes"})

complete_html = "<br/><br/>"+url+"<br/>SGX Nifty<br/>"+str(sgx_table)+"<br/><br/><br/>"+str(table1)
print complete_html
EmailUtil.send_email("SGX Nifty & FII numbers", complete_html, "")
