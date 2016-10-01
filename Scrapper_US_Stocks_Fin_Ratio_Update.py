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
        self.quarter_names = {5: 'Q116', 4: 'Q415', 3: 'Q315', 2: 'Q215',1: 'Q115'}

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

            # eps = float((self.browser.find_element_by_xpath(Constants.yEPS_Xpath).text).replace(",", ""))
            # pe = float((self.browser.find_element_by_xpath(Constants.yPE_Xpath).text).replace(",", ""))
            #
            # pbStr = self.browser.find_element_by_xpath(Constants.yPB_Xpath).text
            # if 'N/A' in pbStr:
            #     pb = None
            # else:
            #     pb = float((pbStr).replace(",", ""))
            #
            # roeStr = self.browser.find_element_by_xpath(Constants.yROE_Xpath).text
            # if 'N/A' in roeStr:
            #     roe = None
            # else:
            #     roe = float((roeStr).replace("%", ""))


            # current_ratioStr = self.browser.find_element_by_xpath(Constants.yCurrentRatio_Xpath).text
            # if 'N/A' in current_ratioStr:
            #     current_ratio = None
            # else:
            #     current_ratio = float((current_ratioStr).replace(",", ""))
            #
            # debtStr = self.browser.find_element_by_xpath(Constants.yDebt_Xpath).text
            # if 'B' in debtStr:
            #     debtStr = debtStr.replace("B", "")
            #     debt = float((debtStr).replace(",", ""))
            #     debt = debt * 1000000000
            # elif 'M' in debtStr:
            #     debtStr = debtStr.replace("M", "")
            #     debt = float((debtStr).replace(",", ""))
            #     debt = debt * 1000000
            # elif 'N/A' in debtStr:
            #     debt = float((debtStr).replace("N/A", "0.0"))
            #     debt = debt * 0
            #     # debt = float((debtStr).replace(",", ""))
            # else:
            #     debt = float((debtStr).replace(",", ""))
            #
            # print 'debt - ', debt
            #
            # debt_equity_ratio_str = self.browser.find_element_by_xpath(Constants.yDebtEquityRatio_Xpath).text
            # print debt_equity_ratio_str
            # if 'N/A' in debt_equity_ratio_str:
            #     debt_equity_ratio = None
            # else:
            #     debt_equity_ratio = float((debt_equity_ratio_str).replace(",", ""))
            # print 'debt_equity_ratio - ', debt_equity_ratio

            # short_name = self.quandlDataObject.getShortName(fullid, table_name)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # delete existing records for the fullid
            # self.quandlDataObject.deleteRecordsForFullid(fullid, table_name)

            try:

                update_now = 'n'
                update_sql = "update " + self.table_name + " set last_price='%s', 52_week_high = '%s' , 52_week_low = '%s', last_price_vs_high52='%s', update_now='%s', last_modified='%s'  where fullid = '%s'  " % (last_price, high52, low52, last_price_vs_high52, update_now, now, fullid);

                self.cur.execute(update_sql)
                print "Update executed for table  " + self.table_name

                # insert_sql = ("INSERT INTO "+table_name+" (fullid, short_name,last_price, eps_ttm,pe,  pb, roe,52_week_high, 52_week_low,last_price_vs_high52, debt,interest, interest_cover, current_ratio,debt_equity_ratio, last_modified, created_on ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s,%s ,%s, %s, %s,%s, %s,%s)")
                # data_quater = (fullid, short_name, last_price, eps, pe, pb, roe, high52, low52, last_price_vs_high52, debt, interest, interest_cover, current_ratio, debt_equity_ratio,  now, now)
                # self.cur.execute(insert_sql, data_quater)
                # print "eps-", eps, " | pe-", pe, " | pb-", pb, " | roe-", roe, " | low52-", low52
                # print "Insert executed for table  ", table_name

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

        # Since everything went fine, update the 'update_now' flag to c
        # if(self.all_good_flag):
        #     self.quandlDataObject.setUpdateNowFlag(fullid,table_name )



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


print "\n\n scrapper_exception_list  - "
print thisObj.scrapper_exception_list

print "\n\n sql_exception_list  - "
print thisObj.sql_exception_list

print "\n Updated Stock list for - ", len(thisObj.updated_stock_list), " Stocks"
print thisObj.updated_stock_list

print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )

# url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
# print "Now run the URL ", url
EmailUtil.send_email("Scrapper Exeption List",thisObj.scrapper_exception_list,  "")
