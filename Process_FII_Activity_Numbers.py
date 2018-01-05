#!/usr/bin/env python
##Amit - purpose - Scrap the screener based on the results dates csv file mentioned below.
# Get all data for Quarter_date and Financial_ratio tables, update Final rating etc.

import urllib.request
from bs4 import BeautifulSoup
import EmailUtil
import requests


class Process_FII_Activity_Numbers:

    def __init__(self):
        self.all_good_flag = True


if __name__ == "__main__":
    thisObj = Process_FII_Activity_Numbers()
    print ("Starting Process_FII_Activity_Numbers")
    try:
        url = "http://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
        
        page = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(page,  "lxml")
        #soup.prettify()
        
        div = soup.find("div",{"class":"fidi_tbl CTR"})
        table1 = div.find("table")
        
        #EmailUtil.send_email("FII Numbers", str(table1), "")
        
        ### SGX Nifty
        url = "http://sgxnifty.org/"
        
        #page = urllib2.urlopen(url).read()
        soup = BeautifulSoup(requests.get(url).content,  "lxml")
        #soup = BeautifulSoup(page)
        
        print ("Starting Process_FII_Activity_Numbers 222")
        sgx_table = soup.find("table",{"class":"indexes"})
        
        print ("Starting Process_FII_Activity_Numbers 333")
        
        complete_html = "<br/><br/>"+url+"<br/>SGX Nifty<br/>"+str(sgx_table)+"<br/><br/><br/>"+str(table1)
        print (complete_html)
        EmailUtil.send_email("SGX Nifty & FII numbers", complete_html, "")
    except Exception as e:
        print ("exception in process", e)
        