##Amit - purpose - Scrap the screener based on the results dates csv file mentioned below.
# Get all data for Quarter_date and Financial_ratio tables, update Final rating etc.

import csv
import DBManager
import platform
from selenium import webdriver
import Module_Final_Rating

import Module_Scrapper_Screener_India_Stocks


class Process_NSE_Based_ResultDates_Screener_ScrapNUpdate:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()
        self.finalRatingModule = Module_Final_Rating.Module_Final_Rating()
        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = './phantomjs.exe'
        else:
            self.PHANTOMJS_PATH = './phantomjs'

        self.browser = webdriver.PhantomJS(self.PHANTOMJS_PATH)
        self.all_good_flag = True




    def getResultsDates(self):

        self.browser.get('https://www.nseindia.com/corporates/corporateHome.html')
        xp1='//*[@id="ext-gen50"]/div/li[1]/ul/li[5]/div/a/span'
        selector = '#ext-gen46'
        corp_info = self.browser.find_element_by_css_selector(selector)
        # corp_info = self.browser.find_element_by_xpath(xp1)
        print corp_info
        corp_info.click()
        # self.browser.get(corp_info)
        #xp2 = '//*[@id="ext-gen50"]/div/li[1]/ul/li[5]/div/a/span'
        selector2 = '#ext-gen50 > div > li:nth-child(1) > ul > li:nth-child(5) > div > a > span'
        res_cal = self.browser.find_element_by_css_selector(selector2)
        print res_cal
        res_cal.click()

        sel3='#ext-gen96'

        drop_down = self.browser.find_element_by_css_selector(sel3)
        print drop_down

    def csv_reader(self,file_obj):
        reader = csv.DictReader(file_obj, delimiter=',')
        nseidString= ''
        count = 0
        for line in reader:
            print(line["Symbol"]),
            print(line["BoardMeetingDate"])
            nseidString+=  "'%s'," % line["Symbol"]
            count +=1
            #call the Sc
        print "Total records in CSV files to be processed- ", count
        print nseidString
        nseidString = nseidString[:-1]
        nseidString = '('+nseidString+')'
        print nseidString
        return nseidString


    def getStockDetails(self,nseidString ):

        ###expalin sql - following sql will only take those nseid where latest quater data is NOT laread scrapped.
        ###This is to avoid scrapping ones alreay have latest data.

        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where nseid in "
        select_sql += "(select nseid from"
        select_sql += "(select nseid from stocksdb.fa_quaterly_data_secondary where  period = '2016-09-30' and quater_sequence=5 and nseid in %s " %nseidString
        select_sql += " union "
        select_sql += "select nseid from stocksdb.fa_quaterly_data where period = '2016-09-30' and quater_sequence=5 and  nseid in %s ) temp ) " %nseidString

        ###Explain Sql - This sql will update all stcoks in the csv file wheather they are already updated or not.
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where nseid in %s " %nseidString

        print select_sql
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            dd["industry_vertical"] = row[3]
            data.append(dd)
        # print data
        return data


# ----------------------------------------------------------------------
thisObj = Process_NSE_Based_ResultDates_Screener_ScrapNUpdate()
csv_path = "BM_Last_15_DaysResults.csv"
#csv_path = "BM_Last_1_WeekResults.csv"
with open(csv_path, "rb") as f_obj:
    nseidString = thisObj.csv_reader(f_obj)
stock_names = thisObj.getStockDetails(nseidString)
thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
thisObj.finalRatingModule.updateAll(stock_names)

