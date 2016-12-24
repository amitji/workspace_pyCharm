#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import Constants
import EmailUtil
import DBManager
from pandas import pandas
import platform
import time
from bs4 import BeautifulSoup
from selenium import webdriver
import unicodedata
import datetime
import QuandlDataModule



class Scrapper_US_Stocks_Fin_Ratio_Update:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.table_name = "fa_financial_ratio_us_stocks"
        self.quandlDataObject = QuandlDataModule.QuandlDataModule()

        self.scrapper_exception_list = []
        self.sql_exception_list = []
        self.updated_stock_list = []

        self.quarter_dates = {5: '2016-06-30', 4: '2016-03-31', 3: '2015-12-31', 2: '2015-09-30', 1: '2015-06-30'}
        self.quarter_names = {5: 'Q216', 4: 'Q116', 3: 'Q415', 2: 'Q315', 1: 'Q215'}

        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = './phantomjs.exe'
        else:
            self.PHANTOMJS_PATH = './phantomjs'

        self.browser = webdriver.PhantomJS(self.PHANTOMJS_PATH)
        self.all_good_flag = True


    def getStockList(self):

        select_sql = "SELECT distinct fullid FROM " + self.table_name + "  order by fullid "
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            data.append(dd)
        # print data
        return data


    #Only update daily changing parameters like last_price, high52, low52 etc
    def updateFinancialRatios(self, row):
        try:
            # nseid = row["nseid"]
            fullid = row["fullid"]

            nseid = fullid.split("NASDAQ:", 1)[1]
            print nseid
            # nseidModified = nseid.replace("&", "")

            url = Constants.yahooUrl_part1 + nseid+Constants.yahooUrl_stats+nseid
            print url
            self.browser.get(Constants.yahooUrl_part1 + nseid+Constants.yahooUrl_stats)
            time.sleep(2)

            last_price = float((self.browser.find_element_by_xpath(Constants.yLastPrice_Xpath).text).replace(",", ""))
            high52 = float((self.browser.find_element_by_xpath(Constants.y52WeekHigh_Xpath).text).replace(",", ""))
            low52 = float((self.browser.find_element_by_xpath(Constants.y52WeekLow_Xpath).text).replace(",", ""))
            last_price_vs_high52 = (last_price / high52) * 100
            # print "last_price_vs_high52 - ", last_price_vs_high52

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # delete existing records for the fullid
            # self.quandlDataObject.deleteRecordsForFullid(fullid, table_name)

            try:

                update_now = 'n'
                update_sql = "update " + self.table_name + " set last_price='%s', 52_week_high = '%s' , 52_week_low = '%s', last_price_vs_high52='%s', update_now='%s', last_modified='%s'  where fullid = '%s'  " % (last_price, high52, low52, last_price_vs_high52, update_now, now, fullid);

                self.cur.execute(update_sql)
                print "Update executed for table  " + self.table_name

            except Exception, e:
                print "error e - ", str(e)
                print "\n******Amit - some excetion executing fa_financial_ratio_us_stocks insert sql 111 for  - " + nseid
                self.sql_exception_list.append(nseid)
                self.all_good_flag = False
                pass

        except Exception, e2:
            print str(e2)
            print "\n******Amit - Exception in Scrapping data for - " + fullid
            self.scrapper_exception_list.append(nseid)
            self.all_good_flag = False
            pass

    def findResultQuarterDate(self, row):
        try:
            fullid = row["fullid"]
            nseid = fullid.split("NASDAQ:", 1)[1]

            #print fullid

            table_name = "fa_quaterly_data_us_stocks"
            # nseidModified = nseid.replace("&", "")

            self.browser.get(Constants.googleUrl + nseid)
            # time.sleep(2)
            self.browser.find_element_by_xpath(Constants.googleFinance_Xpath).click()

            finUrl = self.browser.current_url
            # print finUrl
            self.browser.get(finUrl)
            time.sleep(1)

            date_xpath = '//*[@id="fs-table"]/thead/tr/th[2]'
            date = self.browser.find_element_by_xpath(date_xpath).text
            print nseid, " - " , date[-10:]
            # date = date[-10:]

        except Exception, e2:
            print str(e2)
            print "\n******Amit - Exception in Scrapping data for - " + fullid
            #self.scrapper_exception_list.append(nseid)
            #self.all_good_flag = False
            pass

if __name__ == "__main__":
    while True:
        start_time = time.time()
        thisObj = Scrapper_US_Stocks_Fin_Ratio_Update()

        stock_names= thisObj.getStockList()
        print stock_names
        print "Number of Stocks processing - " , len(stock_names)
        totalCount = len(stock_names)
        count = 0
        #
        for row in stock_names:
            # print row
            count = count + 1
            print "\n\ncalling updateFinancialRatios for - ", row['fullid'], "(", count, "/", totalCount, ")"
            thisObj.updateFinancialRatios(row)
            #thisObj.findResultQuarterDate(row)



        print "\n\n scrapper_exception_list  - ", len(thisObj.scrapper_exception_list), " Stocks"
        print thisObj.scrapper_exception_list

        print "\n\n sql_exception_list  - "
        print thisObj.sql_exception_list

        print "\n Updated Stock list for - ", len(thisObj.updated_stock_list), " Stocks"
        print thisObj.updated_stock_list

        print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )

        # url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
        # print "Now run the URL ", url
        EmailUtil.send_email("Scrapper_US_Stocks:Scrapper Exeption List", thisObj.scrapper_exception_list, "Sleeping for a day, Bye!!")
        print "Sleeping for a day, Bye!!"
        time.sleep(86400)  # 3600 seconds = 1 hour, 24 hrs
