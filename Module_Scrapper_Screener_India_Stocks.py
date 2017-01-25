#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import Constants
import EmailUtil
import DBManager
import platform
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

import datetime

# import FinalRatingModule
import QuandlDataModule
#import Scrapper_US_Stocks_Fin_Ratio_Update
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities




class Module_Scrapper_Screener_India_Stocks:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.quandlDataObject = QuandlDataModule.QuandlDataModule()
        # self.finalRatingModule = FinalRatingModule.FinalRatingModule()


        self.xpaths = self.getXpaths()
        self.scrapper_exception_list = []
        self.sql_exception_list = []
        self.updated_stock_list = []

        self.quarter_dates_v1 = {5: '2016-09-30', 4: '2016-06-30', 3: '2016-03-31', 2: '2015-12-31', 1: '2015-09-30'}
        self.screener_quarter_dates_v1 = {5: 'Sep 2016',4: 'June 2016', 3: 'Mar 2016', 2: 'Dec 2015', 1: 'Sep 2015'}
        self.quarter_names_v1 = {5: 'Q216', 4: 'Q116', 3: 'Q415', 2: 'Q315', 1: 'Q215'}

        self.quarter_dates = {5: '2016-12-31',4: '2016-09-30', 3: '2016-06-30', 2: '2016-03-31', 1: '2015-12-31'}
        self.screener_quarter_dates = {5: 'Dec 2016', 4: 'Sep 2016',3: 'June 2016', 2: 'Mar 2016', 1: 'Dec 2015'}
        self.quarter_names = {5: 'Q316',4: 'Q216', 3: 'Q116', 2: 'Q415', 1: 'Q315'}



        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = './phantomjs.exe'
        else:
            #self.PHANTOMJS_PATH = './phantomjs'
            self.PHANTOMJS_PATH = '/home/shopbindaas/python-workspace/phantomjs'

        self.browser = webdriver.PhantomJS(self.PHANTOMJS_PATH)

        ##############################################################
        self.browser.get('https://www.screener.in/login/')
        username = self.browser.find_element_by_name('username')
        password = self.browser.find_element_by_name('password')
        username.send_keys('amitji@gmail.com')
        password.send_keys('amit1973')
        login_attempt = self.browser.find_element_by_class_name("btn-primary")
        login_attempt.click()


        #self.browser = webdriver.Firefox()
        self.all_good_flag = True

        self.profitIndStr = ""
        self.revenueIndStr = ""


    def getXpaths(self):
        select_sql = "select quater_sequence, revenue_xpath, profit_xpath, opm_xpath, ebit_xpath,date_xpath from stocksdb.xpaths where siteID = 'screener' order by quater_sequence "

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

    def updateQuaterlyData(self, row,qd_table_name,fr_table_name):

        try:
            self.all_good_flag = True
            nseid = row["nseid"]
            fullid = row["fullid"]
            #fullid = 'NSE:INFY'
            #nseid = 'INFY'
            print fullid

            self.revenueIndStr=""
            self.profitIndStr = ""

            table_name = qd_table_name
            # nseidModified = nseid.replace("&", "")


            #self.browser.get('https://www.screener.in')
            #cookies = self.browser.get_cookies()
            #req = requests.get('https://www.screener.in')
            #cookies = req.cookies

            #cookies222 = {}
            # for c in cookies:
            #     name = c['name']
            #     val = c['value']
            #     cookies222[name] = val
            # request = requests.get(Constants.screenerBaseUrl + nseid, cookies=cookies222)


            #self.browser.get('https://www.screener.in')
            try:
                self.browser.get(Constants.screenerBaseUrl + nseid+Constants.screenerBaseUrl_part2)
                time.sleep(5)

                #determine what is latest quarter data available. Based on that you use different dates sets.
                temp_record = self.xpaths[4]
                temp_date = self.browser.find_element_by_xpath(temp_record["date_xpath"]).text
            except Exception, e3:
                print "Exception in Consolidated , try STANDALONE"
                self.browser.get(Constants.screenerBaseUrl + nseid)
                time.sleep(5)
                # determine what is latest quarter data available. Based on that you use different dates sets.
                temp_record = self.xpaths[4]
                temp_date = self.browser.find_element_by_xpath(temp_record["date_xpath"]).text

            short_quarter_date = self.screener_quarter_dates[5]
            short_quarter_date_v1 = self.screener_quarter_dates_v1[5]
            #short_quarter_date_prev_prev = self.screener_quarter_dates[3]
            quarter_version = 2
            if short_quarter_date == temp_date:
                print 'latest quarter results available', short_quarter_date, temp_date
                quarter_version = 2
            elif short_quarter_date_v1 == temp_date:
                print 'one quarter previous results available', short_quarter_date_v1, temp_date
                quarter_version = 1
            else:
                print 'looks 2 quarter old data so try STANDALONE for this stock, last Q date was - ', temp_date
                self.browser.get(Constants.screenerBaseUrl + nseid )
                time.sleep(5)
                temp_record = self.xpaths[4]
                temp_date = self.browser.find_element_by_xpath(temp_record["date_xpath"]).text
                if short_quarter_date == temp_date:
                    print 'latest quarter STANDALONE results available', short_quarter_date, temp_date
                    quarter_version = 2
                else:
                    print 'looks 2 quarter old data so skipping this stock',  temp_date
                    quarter_version = 0
                    self.scrapper_exception_list.append(nseid)
                    self.all_good_flag = False
                    return




            records = []
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            count = 1
            for index,  row in enumerate(self.xpaths):
                try:
                    #count = index+1
                    date = self.browser.find_element_by_xpath(row["date_xpath"]).text
                    #print date[-10:]
                    #date = date[-10:]
                    if quarter_version == 2:
                        quarter_name = self.quarter_names[count]
                        quarter_date = self.quarter_dates[count]
                        short_quarter_date = self.screener_quarter_dates[count]
                    elif quarter_version == 1:
                        quarter_name = self.quarter_names_v1[count]
                        quarter_date = self.quarter_dates_v1[count]
                        short_quarter_date = self.screener_quarter_dates_v1[count]
                    else:
                        print "sholdn't reached here !!!!!!!!!!!!!!!! "

                    if short_quarter_date == date:
                        print 'match'
                        count = count + 1
                    else:
                        # no increament to count so that we can check if data is available for previous quarter (latest results not yet announced)
                        print 'no match'

                        #if latest quster results not there then try the previous one
                        # previous = self.xpaths[index - 1]
                        # prev_rev = float(
                        # prev_date = (self.browser.find_element_by_xpath(previous["revenue_xpath"]).text).replace(",", ""))
                        pass

                    quater_seq = row["quater_sequence"]
                    rev = float((self.browser.find_element_by_xpath(row["revenue_xpath"]).text).replace(",", ""))
                    profit = float((self.browser.find_element_by_xpath(row["profit_xpath"]).text).replace(",", ""))
                    op = float((self.browser.find_element_by_xpath(row["opm_xpath"]).text).replace(",", ""))
                    ebit = float((self.browser.find_element_by_xpath(row["ebit_xpath"]).text).replace(",", ""))

                    profitMargin = (profit/abs(rev))*100  # abs() is used for negative denominator
                    opMargin = (op/abs(rev))*100
                    ebitMargin = (ebit / abs(rev)) * 100

                    #calculate growth rates
                    rev_growth = None
                    rev_growth_rate = None
                    profit_growth = None
                    profit_growth_rate = None

                    if index > 0:
                        previous = self.xpaths[index - 1]
                        prev_rev = float((self.browser.find_element_by_xpath(previous["revenue_xpath"]).text).replace(",", ""))
                        prev_profit = float((self.browser.find_element_by_xpath(previous["profit_xpath"]).text).replace(",", ""))
                        print prev_rev, prev_profit

                        rev_growth = rev-prev_rev
                        rev_growth_rate = (100*rev_growth)/abs(prev_rev) # abs() is used for negative denominator

                        profit_growth = profit - prev_profit
                        profit_growth_rate = (100 * profit_growth) / abs(prev_profit)

                        if rev > prev_rev:
                            self.revenueIndStr += "1"
                        else:
                            self.revenueIndStr += "0"

                        if profit > prev_profit:
                            self.profitIndStr += "1"
                        else:
                            self.profitIndStr += "0"

                    # if index < (no_of_records - 1):
                    #     next_ = xpaths[index + 1]
                    #####
                    print 'quater_seq - ',quater_seq,rev, profit, op, ebit, profitMargin, opMargin, ebitMargin


                    records.append((nseid,fullid, quater_seq,quarter_date, quarter_name, rev, profit,op,ebit, rev_growth,rev_growth_rate, profit_growth, profit_growth_rate, \
                                   profitMargin,  opMargin,  ebitMargin, now, now))

                    # # data_quater = (fullid, quater_seq,quarter_date, quarter_name, rev, profit,op,ebit, rev_growth,rev_growth_rate, profit_growth, profit_growth_rate, \
                    # #                profitMargin,  opMargin,  ebitMargin, now, now)
                    # self.cur.execute(insert_sql, data_quater)
                    #count = count-1

                except Exception, e:
                    print "\n******Amit 000 - Exception in inserting data in ", table_name, " for - " + fullid
                    print str(e)
                    self.scrapper_exception_list.append(nseid)
                    self.all_good_flag = False
                    break
                    #pass

            if (self.all_good_flag):
                # delete existing records for the fullid
                self.quandlDataObject.deleteRecordsForFullid(fullid, table_name)

                #records_list_template = ','.join(['%s'] * len(records))
                insert_sql = (
                "INSERT INTO " + table_name + " (nseid, fullid, quater_sequence, period,quater_name,  revenueC, profitC, opmC, ebidtaC, revenue_growth,revenue_growth_rate, " \
                                              "profit_growth, profit_growth_rate,  profit_margin,operating_profit_margin, ebidt_margin,  last_modified, created_on )" \
                                              "VALUES (%s, %s, %s, %s, %s,%s, %s, %s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s )")
                self.cur.executemany(insert_sql, records)
                self.con.commit()
                #reverse teh indicator string because Quarter Seq was from 1->5 instead 5->1
                print self.profitIndStr
                self.profitIndStr = self.profitIndStr[::-1]
                print self.profitIndStr
                self.profitIndStr = int(self.profitIndStr,2)
                self.revenueIndStr = self.revenueIndStr[::-1]
                self.revenueIndStr = int(self.revenueIndStr,2)
                print "\n******Inserted data in ", table_name, " for - " + fullid

        except Exception, e2:
            print "\n******Amit 111 - Exception in Scrapping data for - " + fullid
            print str(e2)
            self.scrapper_exception_list.append(nseid)
            self.all_good_flag = False
            pass


    def updateFinancialRatios(self, row, qd_table_name, fr_table_name):
        try:
            nseid = row["nseid"]
            fullid = row["fullid"]
            print fullid

            table_name = fr_table_name

            #html = self.browser.page_source
            #print html


            last_price = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[1]/h4[2]/b').text
            if '--' not in last_price:
                last_price = float(last_price[(last_price.rfind(' ') + 1):])
            else:
                last_price = None

            eps = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[1]').text
            if '--' not in eps:
                eps = float(eps[(eps.rfind(' ') + 1):])
            else:
                eps = None

            #print last_price, eps

            pe = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[1]/h4[4]/b').text
            if '--' not in pe:
                pe = float(pe[(pe.rfind(' ') + 1):])
            else:
                pe = None


            pb = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[8]').text
            if '--' not in pb:
                pb = float(pb[(pb.rfind(' ') + 1):])
            else:
                pb = None


            roe = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[7]').text
            if '--' not in roe:
                roe = float(roe[(roe.rfind(' ') + 1):].replace("%", ""))
            else:
                roe = None


            high52 = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[9]').text
            if '--' not in high52:
                high52 = float(high52[(high52.rfind(' ') + 1):])
            else:
                high52 = None


            low52 = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[10]').text
            if '--' not in low52:
                low52 = float(low52[(low52.rfind(' ') + 1):])
            else:
                low52 = None


            debt = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[3]').text
            if 'Cr.' in debt:
                debt = debt.replace(" Cr.", "")
                debt = float(debt[(debt.rfind(' ') + 1):].replace(",", ""))
            else:
                debt = float(debt[(debt.rfind(' ') + 1):].replace(",", ""))

            print 'debt - ', debt

            interest = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[4]').text

            if 'Cr.' in interest:
                interest = interest.replace(" Cr.", "")
                interest = float(interest[(interest.rfind(' ') + 1):].replace(",", ""))
                #interest = interest * 10000000

            interest_coverage = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[5]').text
            if 'Cr.' in interest_coverage:
                interest_coverage = interest_coverage.replace(" Cr.", "")
                interest_coverage = float(interest_coverage[(interest_coverage.rfind(' ') + 1):].replace(",", ""))
                #interest_coverage = interest * 10000000
            elif '--' in interest_coverage:
                interest_coverage = None
            else:
                interest_coverage = float(interest_coverage[(interest_coverage.rfind(' ') + 1):].replace(",", ""))

            current_ratio = ""
            debt_equity_ratio = self.browser.find_element_by_xpath('//*[@id="content"]/div/div/div/section[1]/div[2]/h4[6]').text
            debt_equity_ratio = float(debt_equity_ratio[(debt_equity_ratio.rfind(' ') + 1):])

            last_price_vs_high52 = (last_price / high52) * 100

            short_name = self.quandlDataObject.getShortName(fullid, table_name)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # delete existing records for the fullid
            self.quandlDataObject.deleteRecordsForFullid(fullid, table_name)

            try:
                insert_sql = ("INSERT INTO " + table_name + " (fullid, short_name,last_price, eps_ttm,pe,  pb, roe,52_week_high, 52_week_low," \
                " last_price_vs_high52,profit_ind, revenue_ind, debt,interest, interest_cover, current_ratio,debt_equity_ratio, last_modified, created_on )" \
                                              " VALUES (%s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s,%s ,%s, %s, %s,%s, %s,%s)")

                data_quater = (fullid, short_name, last_price, eps, pe, pb, roe, high52, low52, last_price_vs_high52, self.profitIndStr,self.revenueIndStr, debt, interest, \
                                interest_coverage, current_ratio, debt_equity_ratio, now, now)
                self.cur.execute(insert_sql, data_quater)
                print "eps-", eps, " | pe-", pe, " | pb-", pb, " | roe-", roe, " | low52-", low52
                print "Insert executed for table  ", table_name

            except Exception, e:
                print "\n******Amit 222 - some excetion executing fa_financial_ratio_us_stocks insert sql 111 for  - " + nseid
                print "error e - ", str(e)
                self.sql_exception_list.append(nseid)
                self.all_good_flag = False
                pass

        except Exception, e2:
            print "\n******Amit 333- Exception in Scrapping data for - " + fullid
            print str(e2)
            self.scrapper_exception_list.append(nseid)
            self.all_good_flag = False
            pass

        # Since everything went fine, update the 'update_now' flag to c
        if (self.all_good_flag):
            self.quandlDataObject.setUpdateNowFlag(fullid, table_name, 'n')
            self.quandlDataObject.setIsVideoAvailable(fullid)
            self.quandlDataObject.setVideoAsOldToRecreateNextTime(fullid)
            self.updated_stock_list.append(nseid)

    # def callFinalRatingModule(self,row,qd_table_name,fr_table_name):
    #     self.finalRatingModule.updateFinalRating(row,qd_table_name,fr_table_name)

    def updateAll(self, stock_names):
        print stock_names
        print "Number of Stocks processing - ", len(stock_names)
        start_time = time.time()
        totalCount = len(stock_names)
        count = 0

        for row in stock_names:
            count = count + 1
            print "\n\n****************************************************************************"
            print "\n\ncalling updateQuaterlyData for - ", row['nseid'], "(", count, "/", totalCount, ")"
            enable_for_vendor_data = row['enable_for_vendor_data']
            if enable_for_vendor_data == '1':
                qd_table_name = "fa_quaterly_data"
                fr_table_name = "fa_financial_ratio"
                self.updateQuaterlyData(row, qd_table_name, fr_table_name)
                if (self.all_good_flag):
                    print "\n\ncalling updateFinancialRatios for - ", row['nseid'], "(", count, "/", totalCount, ")"
                    self.updateFinancialRatios(row, qd_table_name, fr_table_name)
                    # self.finalRatingModule.updateFinalRating(row, qd_table_name, fr_table_name)
                else:
                    print "*** Amit -  Since updateQuaterlyData FAILED , not calling updateFinancialRatios.Move to next one "
            elif enable_for_vendor_data == '2':
                qd_table_name = "fa_quaterly_data_secondary"
                fr_table_name = "fa_financial_ratio_secondary"
                self.updateQuaterlyData(row, qd_table_name, fr_table_name)
                if (self.all_good_flag):
                    print "\n\ncalling updateFinancialRatios for - ", row['nseid'], "(", count, "/", totalCount, ")"
                    self.updateFinancialRatios(row, qd_table_name, fr_table_name)
                    # self.finalRatingModule.updateFinalRating(row, qd_table_name, fr_table_name)
                else:
                    print "*** Amit -  Since updateQuaterlyData FAILED , not calling updateFinancialRatios.Move to next one "

        self.browser.quit();
        print "\n\n scrapper_exception_list  - "
        print self.scrapper_exception_list

        print "\n\n sql_exception_list  - "
        print self.sql_exception_list

        print "\n Updated Stock list for - ", len(self.updated_stock_list), " Stocks"
        print self.updated_stock_list

        print("\n\nTime Taken --- in minutes ---", int((time.time() - start_time)) / 60)

        print "\n\n ************************************************************************"
        print "NOW RUN THE FINAL RATING PROCESS FOR THE SAME SET OF STOCKS"
        print "************************************************************************"
        # url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
        # print "Now run the URL ", url
        EmailUtil.send_email_as_text("Scrapper Exeption List - ", self.scrapper_exception_list, "")




