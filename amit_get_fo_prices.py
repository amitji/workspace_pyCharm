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

        self.browser.get(base_url + 'GMRINFRA')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price)
        value =  int(45000) * float(diff)
        print ('GMRINFRA - ',last_price, last_price2 ,'<>',diff, '<>', value)
#        
        self.browser.get(base_url + 'JINDALSTEL')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price) 
        value =  int(4500) * float(diff)
        print ('JINDALSTEL  - ',last_price, last_price2, '<>',diff, '<>', value)

#        self.browser.get(base_url + 'GLENMARK')
#        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
#        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
#        diff = "{0:.2f}".format(last_price2-last_price) 
#        value =  int(700) * float(diff)
#        print ('GLENMARK  - ',last_price, last_price2, '<>',diff, '<>', value)
#
        self.browser.get(base_url + 'AUROPHARMA')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price) 
        value =  int(800) * float(diff)
        print ('AUROPHARMA  - ',last_price, last_price2, '<>',diff, '<>', value)
#
#        self.browser.get(base_url + 'FEDERALBNK')
#        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
#        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
#        diff = "{0:.2f}".format(last_price2-last_price) 
#        value =  int(5500) * float(diff)
#        print ('FEDERALBNK  - ',last_price, last_price2, '<>',diff, '<>', value)

        self.browser.get(base_url + 'ASHOKLEY')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price) 
        value =  int(7000) * float(diff)
        print ('ASHOKLEY  - ',last_price, last_price2, '<>',diff, '<>', value)
        
#        self.browser.get(base_url + 'CADILAHC')
#        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
#        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
#        diff = "{0:.2f}".format(last_price2-last_price) 
#        value =  int(1600) * float(diff)
#        print ('CADILAHC  - ',last_price, last_price2, '<>',diff, '<>', value)
#        
        self.browser.get(base_url + 'YESBANK')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price) 
        value =  int(1750) * float(diff)
        print ('YESBANK  - ',last_price, last_price2, '<>',diff, '<>', value)
  

        self.browser.get(base_url + 'SUZLON')
        last_price = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[2]/td[10]').text)
        last_price2 = float(self.browser.find_element_by_xpath('//*[@id="tab26Content"]/table/tbody/tr[3]/td[10]').text)
        diff = "{0:.2f}".format(last_price2-last_price) 
        value =  int(30000) * float(diff)
        print ('SUZLON  - ',last_price, last_price2, '<>',diff, '<>', value)
        
        

    def in_between(self,now, start, end):
        if start <= end:
            return start <= now < end
        else:  # over midnight e.g., 23:30-04:15
            return start <= now or now < end

        
if __name__ == "__main__":
    obj =   GetFOPrices()
    minutes_count = 0
    
#    obj.getPrice()
    
    while (obj.in_between(datetime.now().time(), time(9,00), time(16,00))):

        obj.getPrice()
        minutes_count = minutes_count+1
        print ("\n*** Amit Sleeping for 2 minute, remaining loops (420-x)- ", 420- minutes_count, " | Time - ", datetime.now())
        t.sleep(300)
    
    print ("\n*** Amit Exiting the NSE Get FO process...TIME is  - ", datetime.now().time())        
      