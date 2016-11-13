

import csv
import DBManager
import platform
from selenium import webdriver
import FinalRatingModule
import datetime

import Module_Scrapper_Screener_India_Stocks


class NSE_Result_Calendar_Update_Process:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()
        self.finalRatingModule = FinalRatingModule.FinalRatingModule()
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
        records = []
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for line in reader:
            print(line["Symbol"]),
            print(line["BoardMeetingDate"])
            nseidString+=  "'%s'," % line["Symbol"]
            count +=1
            nseid = line["Symbol"]
            name = line["Company"]
            industry = line["Industry"]
            purpose = line["Purpose"]
            result_date = line["BoardMeetingDate"]


            records.append((nseid,name,industry,purpose,result_date,now))

        #update the result_calendar table
        insert_sql = (
            "INSERT INTO result_calendar  (  nseid, name, result_date, Industry, purpose, last_modified, )" \
                                          "VALUES (%s, %s, %s, %s,%s, %s )")
        self.cur.executemany(insert_sql, records)
        self.con.commit()

        print "Total records in CSV files to be processed- ", count
        print nseidString
        nseidString = nseidString[:-1]
        nseidString = '('+nseidString+')'
        print nseidString
        return nseidString


    def getStockDetails(self,nseidString ):
        select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where nseid in %s " %nseidString

        # print select_sql
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            data.append(dd)
        # print data
        return data


# ----------------------------------------------------------------------
thisObj = NSE_Results_Date_Scrapper()
csv_path = "BM_Last_15_DaysResults.csv"
with open(csv_path, "rb") as f_obj:
    nseidString = thisObj.csv_reader(f_obj)
stock_names = thisObj.getStockDetails(nseidString)
#thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
thisObj.finalRatingModule.updateAll(stock_names)

