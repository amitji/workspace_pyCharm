
from selenium import webdriver
import time
from custom_logging import logger
import env
#import dataProcessing
import os
from collections import OrderedDict
import win32com.client
import datetime
import DBManager
import Constants

print __name__

class Module_Screener_Excel_Data:

    def __init__(self, dir_name):

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.date_map= {'03/31/13 00:00:00' : '2013-03-31', '03/31/14 00:00:00':'2014-03-31', '03/31/15 00:00:00':'2015-03-31', '03/31/16 00:00:00':'2016-03-31', '03/31/17 00:00:00':'2017-03-31'}
        """"Setting browser preferences to handle download pop-up"""
        self.profile = webdriver.FirefoxProfile()

        self.profile.set_preference('browser.download.folderList', 2)
        self.profile.set_preference('browser.download.panel.shown', True)
        self.profile.set_preference('browser.download.manager.showWhenStarting', True)
        self.profile.set_preference('browser.download.dir', dir_name)
        self.profile.set_preference('browser.helperApps.neverAsk.saveToDisk',
                                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        self.profile.set_preference('browser.helperApps.neverAsk.openFile',
                                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        self.profile.set_preference('browser.download.manager.showAlertOnComplete', False)
        self.profile.set_preference('browser.download.manager.closeWhenDone', False)
        self.profile.set_preference('browser.helperApps.alwaysAsk.force', False)

        self.xl = win32com.client.Dispatch('Excel.Application')



    def login(self):

        ##############################################################
        try:
            self.driver = webdriver.Firefox(executable_path=env.BROWSER_PATH, firefox_profile=self.profile)
            #self.driver.set_window_size(0, 0)
            #self.driver.set_window_position(1920,1080)
            self.driver.set_window_position(-2000, 0)
            self.driver.get('https://www.screener.in/login/')

        except Exception as e:
            logger.exception(e)
            self.driver.quit()
        #username = self.driver.find_element_by_name('username')

        time.sleep(2)
        username = self.driver.find_element_by_id('id_username')

        #password = self.driver.find_element_by_name('password')
        password = self.driver.find_element_by_id('id_password')
        # username.send_keys('amitji@gmail.com')
        # password.send_keys('amit1973')
        username.send_keys(Constants.screener_userid)
        password.send_keys(Constants.screener_pwd)

        logger.info('Logging in')
        self.driver.find_element_by_tag_name('button').submit()
        time.sleep(2)

        # login_attempt = self.driver.find_element_by_class_name("btn-primary")
        # login_attempt.click()

    def __enter__(self):
        logger.debug('Inside __enter__()')
        return self

    def __exit__(self):
        self.driver.quit()
        logger.info('Webdriver handler closed')


    def export_to_excel(self,nseid, type):

        logger.info("******************************************************")
        logger.info('Clicking excel: ' + self.driver.current_url)
        try:
            if type == 'C':
                url = env.URL + nseid+"/consolidated"
            else:
                url = env.URL+nseid;

            print "company url - ",url
            self.driver.get(url)

            # company = self.driver.find_element_by_tag_name('input')
            # company.send_keys(stock_name)
            # time.sleep(2)
            # company_list = self.driver.find_element_by_class_name('dropdown-menu')
            # company_list.click()
            # self.driver.find_element_by_tag_name('button').click()
            time.sleep(2)

            self.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(3)

            #logger.info(self.driver.current_url)
            logger.info('CLicked...Check')
            #self.__exit__()

        except Exception as e:
            logger.exception(e)

        finally:
            logger.info("******************************************************")


    def getStockFundamentalData(self, nseid, type):
        self.login()
        self.export_to_excel(nseid, type)
        time.sleep(2)
        return

    def readAllFilesData(self, nseid,fullid, dir_name, type):
        records= []
        for file in os.listdir(dir_name):
            if  not file.startswith("~"):
                try:
                    filename = os.path.join(dir_name, file)
                    print filename
                    #read from data sheet
                    sheet='Data Sheet'
                    wb = self.xl.Workbooks.Open(Filename=filename, ReadOnly=1, Editable=True)
                    ws = wb.Worksheets(sheet)
                    no_of_shares = ws.Range('B6').value #* 10000000  (data comes in Crore)

                    #print no_of_shares

                    sheet = 'Profit & Loss'
                    #cell_range = OrderedDict([('A03', 'J03'), ('A12', 'J12'), ('A13', 'J13'), ('A14', 'J14'), ('A15', 'J15')])
                    cell_range = OrderedDict([('H03', 'H15'),('I03', 'I15'),('J03', 'J15'),('K03', 'K15'),('L03', 'L15')])


                    ws = wb.Worksheets(sheet)
                    seq_no =0
                    for k, val in cell_range.items():
                        seq_no = seq_no+1
                        #print (ws.Range(k + ':' + val).Value)
                        one_record = [nseid,fullid ]
                        temp_record = ws.Range(k + ':' + val).Value
                        print 'date - ', temp_record[0][0]
                        if temp_record[0][0] == 'Trailing':
                            #now_datetime = '2017-03-31 00:00:00'
                            now_datetime = Constants.latest_period
                        else:
                            #Amit TODO: excel sheet has differet dates as headers so below code partially works..
                            now_datetime = self.date_map.get(str(temp_record[0][0]))

                        print 'now_datetime - ', now_datetime

                        one_record.append(seq_no)
                        one_record.append(now_datetime)
                        one_record.append(temp_record[9][0]) #profit row
                        one_record.append(temp_record[1][0]) # sales row
                        one_record.append(temp_record[10][0]) # eps
                        one_record.append(temp_record[11][0])
                        one_record.append(temp_record[12][0])
                        one_record.append(no_of_shares)
                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        one_record.append(type)
                        one_record.append(now)
                        one_record.append(now)
                        print one_record
                        records.append(one_record)

                except Exception as e:
                    logger.exception(e)

                else:
                    wb.Close(True)
        self.saveToDB(records, fullid)
        return

    def saveToDB(self, records, fullid):

        delete_sql = "delete from stock_forecasting_pe_eps_past_years_data where fullid = '%s'" % fullid
        self.cur.execute(delete_sql)
        self.con.commit()

        insert_sql = ("INSERT INTO stock_forecasting_pe_eps_past_years_data (nseid,fullid, seq_no, fin_year, profit,revenue, eps, pe, price,  no_of_shares ,type, last_modified, created_on ) VALUES (%s,%s,%s,%s,%s, %s, %s, %s,%s, %s, %s,%s, %s)")

        self.cur.executemany(insert_sql, records)
        self.con.commit()
        #Now set the update_now flag to 'n' so that its not processed again for the same quarter
        self.updateFlag(fullid)


    def updateFlag(self, fullid):
        updateSql = "update stock_names_for_forecasting set update_now = '%s' where fullid = '%s' " % (
            'n', fullid)
        # print updateSql
        self.cur.execute(updateSql)
        self.con.commit()