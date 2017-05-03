
from selenium import webdriver
import time
from custom_logging import logger
import env
import dataProcessing
import os

print __name__

class Module_Screener_Excel_Data:

    def __init__(self, url=env.URL, browser_path=env.BROWSER_PATH):

        """"Setting browser preferences to handle download pop-up"""

        self.profile = webdriver.FirefoxProfile()

        self.profile.set_preference('browser.download.folderList', 2)
        self.profile.set_preference('browser.download.panel.shown', True)
        self.profile.set_preference('browser.download.manager.showWhenStarting', True)
        self.profile.set_preference('browser.download.dir', env.DOWNLOAD_DIR)
        self.profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        self.profile.set_preference('browser.helperApps.neverAsk.openFile', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        self.profile.set_preference('browser.download.manager.showAlertOnComplete', False)
        self.profile.set_preference('browser.download.manager.closeWhenDone', False)
        self.profile.set_preference('browser.helperApps.alwaysAsk.force', False)


        #self.profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application / xls;text / csv')
        #self.profile.set_preference('browser.helperApps.neverAsk.openFile','application / xls;text / csv')




    def login(self):

        ##############################################################
        try:
            self.driver = webdriver.Firefox(executable_path=env.BROWSER_PATH, firefox_profile=self.profile)
            self.driver.get('https://www.screener.in/login/')

        except Exception as e:
            logger.exception(e)
            self.driver.quit()
        #username = self.driver.find_element_by_name('username')

        time.sleep(2)
        username = self.driver.find_element_by_id('id_username')

        #password = self.driver.find_element_by_name('password')
        password = self.driver.find_element_by_id('id_password')
        username.send_keys('amitji@gmail.com')
        password.send_keys('amit1973')
        logger.info('Logging in')
        self.driver.find_element_by_tag_name('button').submit()
        time.sleep(2)

        # login_attempt = self.driver.find_element_by_class_name("btn-primary")
        # login_attempt.click()

    def __enter__(self):
        logger.debug('Inside __enter__()')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()
        logger.info('Webdriver handler closed')

    def export_to_excel_OLD(self, email, password, security='INFY'):
        self.email = email
        self.password = password
        self.security = security
        self.driver.get(env.URL)
        logger.info("******************************************************")
        logger.info('Clicking excel: ' + self.driver.current_url)
        try:
            self.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(3)

            logger.info('Registration Page: ' + self.driver.current_url)
            self.driver.find_element_by_link_text('Login here').click()
            time.sleep(2)

            logger.info('Entering login credentials: ' + self.driver.current_url)
            uname = self.driver.find_element_by_id('id_username')
            uname.send_keys(self.email)
            pwd = self.driver.find_element_by_id('id_password')
            pwd.send_keys(self.password)
            logger.info('Logging in')
            self.driver.find_element_by_tag_name('button').submit()
            time.sleep(2)

            logger.info(self.driver.current_url)
            logger.info('Clicking for excel')
            company = self.driver.find_element_by_tag_name('input')
            company.send_keys(self.security)
            time.sleep(2)
            company_list = self.driver.find_element_by_class_name('dropdown-menu')
            company_list.click()
            self.driver.find_element_by_tag_name('button').click()
            time.sleep(2)

            logger.info(self.driver.current_url)
            script_list = self.driver.find_elements_by_tag_name('h4')
            script_data = map(lambda x: x.text, script_list)
            logger.info(script_data)

            self.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(2)
            logger.info(self.driver.current_url)
            logger.info('CLicked...Check')

        except Exception as e:
            logger.exception(e)

        finally:
            logger.info("******************************************************")

    def export_to_excel(self,stock_name):

        logger.info("******************************************************")
        logger.info('Clicking excel: ' + self.driver.current_url)
        try:
            company = self.driver.find_element_by_tag_name('input')
            company.send_keys(stock_name)
            time.sleep(2)
            company_list = self.driver.find_element_by_class_name('dropdown-menu')
            company_list.click()
            self.driver.find_element_by_tag_name('button').click()
            time.sleep(2)

            self.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(3)

            #logger.info(self.driver.current_url)
            logger.info('CLicked...Check')

        except Exception as e:
            logger.exception(e)

        finally:
            logger.info("******************************************************")


    def getStockFundamentalData(self, stock_names):
        self.login()
        #self.export_to_excel_OLD("amitji@gmail.com","amit1973")


        for row in stock_names:
            self.export_to_excel(row['nseid'])
            time.sleep(2)
        return

    def readAllFilesData(self):

        for file in os.listdir(env.DOWNLOAD_DIR):
            if file.endswith(".xlsx"):
                print(os.path.join(env.DOWNLOAD_DIR, file))
