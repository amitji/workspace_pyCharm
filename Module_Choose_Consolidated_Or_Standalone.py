
import Constants as const
import EmailUtil
import DBManager
import platform
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

import datetime



class Module_Choose_Consolidated_Or_Standalone:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.xpaths = self.getXpaths()

        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = const.PHANTOMJS_PATH
        else:
            #self.PHANTOMJS_PATH = './phantomjs'
            self.PHANTOMJS_PATH = '/home/shopbindaas/python-workspace/phantomjs'

        self.browser = webdriver.PhantomJS(self.PHANTOMJS_PATH)

        ##############################################################
        self.browser.get('https://www.screener.in/login/')
        username = self.browser.find_element_by_name('username')
        password = self.browser.find_element_by_name('password')
        username.send_keys(const.screener_userid)
        password.send_keys(const.screener_pwd)
        login_attempt = self.browser.find_element_by_class_name("button-primary")
        login_attempt.click()


        #self.browser = webdriver.Firefox()
        self.all_good_flag = True
        self.all_good_stock_names = list()

        self.profitIndStr = ""
        self.revenueIndStr = ""


    def getXpaths(self):
        select_sql = "select quater_sequence, revenue_xpath, profit_xpath, opm_xpath, ebit_xpath,date_xpath from stocksdb.xpaths "
        select_sql += " where is_active='y' and siteID = 'screener' and quater_sequence >= '%s'  order by quater_sequence " %const.min_xpath_quarter_seq

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        xpaths = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["quater_sequence"] = row[0]
            dd["revenue_xpath"] = row[1]
            dd["profit_xpath"] = row[2]
            dd["opm_xpath"] = row[3]
            dd["ebit_xpath"] = row[4]
            dd["date_xpath"] = row[5]
            xpaths.append(dd)
        # print data
        return xpaths

    def chooseConsolidatedOrStandalone(self, nseid):

            self.all_good_flag = True
            #nseid = row["nseid"]

            if(len(self.xpaths) < const.number_of_quarters_to_process):
                number_of_quarters_to_process = len(self.xpaths)
            else:
                number_of_quarters_to_process = const.number_of_quarters_to_process
            #self.browser.get('https://www.screener.in')
            try:
                self.browser.get(const.screenerBaseUrl + nseid+const.screenerBaseUrl_part2)
                time.sleep(2)
                #determine what is latest quarter data available. Based on that you use different dates sets.
                temp_record = self.xpaths[number_of_quarters_to_process-1]
                temp_date = self.browser.find_element_by_xpath(temp_record["date_xpath"]).text
                print (temp_date)
                return 'C'
            except Exception as e3:
                #This means consolidate data is not there
                print ("Exception in Consolidated so work with STANDALONE data")
                return 'S'

    def __enter__(self):
        return self

    def __exit__(self):
        self.browser.quit()
