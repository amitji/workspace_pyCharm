
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



class Scrapper_Two:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.quandlDataObject = QuandlDataModule.QuandlDataModule()

        self.scrapper_exception_list = []
        self.fr_exception_list = []
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
        select_sql = "select * from (SELECT sn.fullid, sn.nseid FROM stocksdb.fa_quaterly_data_us_stocks qd, stocksdb.stock_names sn where qd.quater_sequence = 5 " \
                     " and (qd.quater_name = '" + Constants.previous_quarter + "' or qd.quater_name = '" + Constants.prev_to_previous_quarter + "' ) "
        select_sql += " and qd.fullid=sn.fullid "
        select_sql += "union SELECT sn.fullid, sn.nseid FROM stocksdb.stock_names sn where exchange = 'NASDAQ' and update_now = 'y' "
        select_sql += ") sn order by nseid"

        # select_sql = "select fullid, nseid from stocksdb.stock_names sn where exchange='NASDAQ' and update_now='y' "
        # select_sql = "select fullid, nseid from stocksdb.stock_names sn where exchange='NASDAQ' "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            data.append(dd)
        # print data
        return data


    def updateFinancialRatios(self, row):
        try:
            nseid = row["nseid"]
            fullid = row["fullid"]
            print fullid

            table_name = "fa_financial_ratio_us_stocks"
            url = Constants.yahooUrl_part1 + nseid+Constants.yahooUrl_stats+nseid
            print url
            self.browser.get(Constants.yahooUrl_part1 + nseid+Constants.yahooUrl_stats)
            time.sleep(2)

            last_price = float((self.browser.find_element_by_xpath(Constants.yLastPrice_Xpath).text).replace(",", ""))
            eps = float((self.browser.find_element_by_xpath(Constants.yEPS_Xpath).text).replace(",", ""))
            pe = float((self.browser.find_element_by_xpath(Constants.yPE_Xpath).text).replace(",", ""))
            pb = float((self.browser.find_element_by_xpath(Constants.yPB_Xpath).text).replace(",", ""))
            roe = float((self.browser.find_element_by_xpath(Constants.yROE_Xpath).text).replace("%", ""))
            high52 = float((self.browser.find_element_by_xpath(Constants.y52WeekHigh_Xpath).text).replace(",", ""))
            low52 = float((self.browser.find_element_by_xpath(Constants.y52WeekLow_Xpath).text).replace(",", ""))
            debtStr = self.browser.find_element_by_xpath(Constants.yDebt_Xpath).text
            print 'debtStr - ', debtStr
            if 'B' in debtStr:
                debtStr = debtStr.replace("B", "")
                debt = float((debtStr).replace(",", ""))
                debt = debt * 1000000000
            elif 'M' in debtStr:
                debtStr = debtStr.replace("M", "")
                debt = float((debtStr).replace(",", ""))
                debt = debt * 1000000
            elif 'N/A' in debtStr:
                debt = float((debtStr).replace("N/A", "0.0"))
                debt = debt * 0
                # debt = float((debtStr).replace(",", ""))
            else:
                debt = float((debtStr).replace(",", ""))

            print 'debt - ', debt

            debt_equity_ratio_str = self.browser.find_element_by_xpath(Constants.yDebtEquityRatio_Xpath).text
            print debt_equity_ratio_str
            if 'N/A' in debt_equity_ratio_str:
                debt_equity_ratio = None
            else:
                debt_equity_ratio = float((debt_equity_ratio_str).replace(",", ""))

            print 'debt_equity_ratio - ', debt_equity_ratio

            print last_price

            last_price_vs_high52 = (last_price / high52) * 100
            print "last_price_vs_high52 - ", last_price_vs_high52

            short_name = self.quandlDataObject.getShortName(fullid, table_name)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ##Interest needs to be fetched from 'Financial' tab
            # url = Constants.yahooUrl_part1 + nseid+Constants.yahooUrl_financial+nseid
            # print url
            # self.browser.get(url)
            # time.sleep(2)
            # interest = float((self.browser.find_element_by_xpath(Constants.yInterest_Xpath).text).replace(",", ""))
            interest = ""
            interest_cover = ""

            # delete existing records for the fullid
            self.quandlDataObject.deleteRecordsForFullid(fullid, table_name)

            try:
                insert_sql = ("INSERT INTO "+table_name+" (fullid, short_name,last_price, eps_ttm,pe,  pb, roe,52_week_high, 52_week_low,last_price_vs_high52, debt,interest, interest_cover, debt_equity_ratio, last_modified, created_on ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s ,%s, %s, %s,%s, %s,%s)")
                data_quater = (fullid, short_name, last_price, eps, pe, pb, roe, high52, low52, last_price_vs_high52, debt, interest, interest_cover, debt_equity_ratio,  now, now)
                self.cur.execute(insert_sql, data_quater)
                print "eps-", eps, " | pe-", pe, " | pb-", pb, " | roe-", roe, " | low52-", low52
                print "Insert executed for table  ", table_name

            except Exception, e:
                print "error e - ", str(e)
                print "\n******Amit - some excetion executing fa_financial_ratio_us_stocks insert sql 111 for  - " + nseid
                self.fr_exception_list.append(nseid)
                self.all_good_flag = False
                pass




        except Exception, e2:
            print str(e2)
            print "\n******Amit - Exception in Scrapping data for - " + fullid
            self.scrapper_exception_list.append(nseid)
            self.all_good_flag = False
            pass

        # Since everything went fine, update the 'update_now' flag to c
        if(self.all_good_flag):
            self.quandlDataObject.setUpdateNowFlag(fullid,table_name )


start_time = time.time()
thisObj = Scrapper_Two()

stock_names= thisObj.getStockList()
print stock_names
print "Number of Stocks processing - " , len(stock_names)
totalCount = len(stock_names)
count = 0
#
for row in stock_names:
    # print row
    count = count + 1
    print "\n\ncalling updateQuaterlyData for - ", row['nseid'], "(", count, "/", totalCount, ")"
    #thisObj.updateQuaterlyData(row)
    thisObj.updateFinancialRatios(row)


print "\n\n scrapper_exception_list  - "
print thisObj.scrapper_exception_list

print "\n\n fr_exception_list  - "
print thisObj.fr_exception_list

print "\n Updated Stock list for - ", len(thisObj.updated_stock_list), " Stocks"
print thisObj.updated_stock_list

print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )

# url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
# print "Now run the URL ", url
EmailUtil.send_email("Scrapper ",thisObj.scrapper_exception_list,  "Above is exception list")
