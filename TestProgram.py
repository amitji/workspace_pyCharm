# -*- coding: utf-8 -*-
"""
Created on Tue Mar 20 13:50:45 2018

@author: amimahes
"""

"""
import Module_Screener_Excel_Data
dir_name = "c:\\downloads\\"
url="https://finance.google.com/finance?q=NSE:NCC&output=json"
module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data(dir_name)
module_Screener_Excel_Data.testUrl(url)

"""

"""
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from lxml import html

url="https://finance.google.com/finance?q=NSE:NCC"
PHANTOMJS_PATH = './phantomjs.exe'        
browser = webdriver.PhantomJS(PHANTOMJS_PATH)
browser.get(url)
tree = html.fromstring(browser.page_source)
xpath="/html/body/div[1]/div/div[4]/div[2]/div/div[2]/div/div/div[2]/div[2]/div[1]/div[1]/div[1]/span/span/span[1]"
#xpath = '//*[@id="price-panel"]'
print (tree.xpath(xpath))

price = browser.find_element_by_xpath(xpath).text

print(price)

#print (tree.xpath('/html/body/div[1]/div/div[4]/div[2]/div/div[2]/div/div/div[2]/div[2]/div[1]/div[1]/div[1]/span/span/span[1]'))

"""

import json

import requests

rsp = requests.get('https://finance.google.com/finance?q=AAPL&output=json')

if rsp.status_code in (200,):

    # This magic here is to cut out various leading characters from the JSON 
    # response, as well as trailing stuff (a terminating ']\n' sequence), and then
    # we decode the escape sequences in the response
    # This then allows you to load the resulting string
    # with the JSON module.
    fin_data = json.loads(rsp.content[6:-2].decode('unicode_escape'))

    # print out some quote data
    print('Opening Price: {}'.format(fin_data['op']))
    print('Price/Earnings Ratio: {}'.format(fin_data['pe']))
    print('52-week high: {}'.format(fin_data['hi52']))
    print('52-week low: {}'.format(fin_data['lo52']))
    
    

