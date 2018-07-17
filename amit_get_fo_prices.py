# -*- coding: utf-8 -*-

import nsepy as nse
from datetime import datetime, time
import Constants as const
import DBManager
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time as t
import sys, os
import platform

class GetFOPrices:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.scrapper_exception_list = []
        self.sql_exception_list = []
        self.updated_stock_list = []


        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = './phantomjs.exe'
        else:
            #self.PHANTOMJS_PATH = './phantomjs'
            self.PHANTOMJS_PATH = '/home/shopbindaas/python-workspace/phantomjs'

        self.browser = webdriver.PhantomJS(self.PHANTOMJS_PATH)    
        
        
    def getPrice(self):
        base_url = 'https://www.nseindia.com/live_market/dynaContent/live_watch/fomwatchsymbol.jsp?Fut_Opt=Futures&key='
#        stocks = ['FEDERALBNK','GMRINFRA','JINDALSTEL','AUROPHARMA','ASHOKLEY','SUZLON']
#        stocks = {'FEDERALBNK':5500,'GMRINFRA':45000,'JINDALSTEL':4500,'NCC':8000,'CADILAHC': 1600, \
#                      'SREINF':5000, 'DCBBANK':4500, 'AUROPHARMA':900, 'ASHOKLEY':7000,'SUZLON':30000}
#        stocks = {'FEDERALBNK':5500,'GMRINFRA':45000,'JINDALSTEL':4500,'NCC':8000,'CADILAHC': 1600, \
#                      'SREINFRA':5000, 'DCBBANK':4500, 'NIFTY':75}
        stocks = {'NCC':8000}
                        
        
        for stk in stocks:
            try:
#                print (stk, ' lot size - ', stocks[stk])
                lot =  stocks[stk]
                self.browser.get(base_url + stk)
                last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text.replace(',',''))
                last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text.replace(',',''))
                diff = "{0:.2f}".format(last_price2-last_price)
                value =  int(lot) * float(diff)
                print ("\n",stk+' - ',last_price, last_price2 ,'<>',diff, '<>', value)
#                print(base_url + stk)
            except Exception as e:
                print ("\n******Amit exception in getFOPrice for  - \n ", stk)
                print (str(e))
                pass
                
        
        
   
        

    def in_between(self,now, start, end):
        if start <= end:
            return start <= now < end
        else:  # over midnight e.g., 23:30-04:15
            return start <= now or now < end

        
if __name__ == "__main__":
    obj =   GetFOPrices()
    minutes_count = 0
    
#    obj.getPrice()
    
    while (obj.in_between(datetime.now().time(), time(9,30), time(15,40))):

        obj.getPrice()
        minutes_count = minutes_count+1
        print ("\n*** Amit Sleeping for 1.5 minute, remaining loops (420-x)- ", 420- minutes_count, " | Time - ", datetime.now())
        t.sleep(30)
    
    print ("\n*** Amit Exiting the NSE Get FO process...TIME is  - ", datetime.now().time())        
      