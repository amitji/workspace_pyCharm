from selenium import webdriver
import time
from custom_logging import logger
import env
import dataProcessing

print __name__

class scraping():
    profile = ''
    driver = ''
    def __init__(self, url=env.URL, browser_path=env.BROWSER_PATH):
        """"Setting browser preferences to handle download pop-up"""
        scraping.profile = webdriver.FirefoxProfile()
        #scraping.profile = webdriver.FirefoxProfile('C:\\Users\\amahe6\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\wujk8139.default')
        #scraping.profile = webdriver.FirefoxProfile('C:\\Users\\amahe6\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\wujk8139.default')


        scraping.profile.set_preference('browser.download.folderList', 2)
        scraping.profile.set_preference('browser.download.panel.shown', False)
        scraping.profile.set_preference('browser.download.manager.showWhenStarting', False)
        scraping.profile.set_preference('browser.download.dir', 'c:\\downloads')
        scraping.profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        scraping.profile.set_preference('browser.helperApps.neverAsk.openFile', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        # scraping.profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv,application/x-msexcel,application/excel,application/x-excel,application/vnd.ms-excel,image/png,image/jpeg,text/html,text/plain,application/msword,application/xml')
        # scraping.profile.set_preference('browser.helperApps.neverAsk.openFile', 'text/csv,application/x-msexcel,application/excel,application/x-excel,application/vnd.ms-excel,image/png,image/jpeg,text/html,text/plain,application/msword,application/xml')

        try:
            scraping.driver = webdriver.Firefox(executable_path=browser_path, firefox_profile=scraping.profile)
            #scraping.driver = webdriver.Firefox(scraping.profile)
            scraping.driver.get(url)
        except Exception as e:
            logger.exception(e)
            scraping.driver.quit()

    def __enter__(self):
        logger.debug('Inside __enter__()')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        scraping.driver.quit()
        logger.info('Webdriver handler closed')

    def export_to_excel(self, email, password, security='INFY'):
        self.email = email
        self.password = password
        self.security = security
        logger.info("******************************************************")
        logger.info('Clicking excel: ' + scraping.driver.current_url)
        try:
            scraping.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(3)

            logger.info('Registration Page: ' + scraping.driver.current_url)
            scraping.driver.find_element_by_link_text('Login here').click()
            time.sleep(2)

            logger.info('Entering login credentials: ' + scraping.driver.current_url)
            uname = scraping.driver.find_element_by_id('id_username')
            uname.send_keys(self.email)
            pwd = scraping.driver.find_element_by_id('id_password')
            pwd.send_keys(self.password)
            logger.info('Logging in')
            scraping.driver.find_element_by_tag_name('button').submit()
            time.sleep(2)

            logger.info(scraping.driver.current_url)
            logger.info('Clicking for excel')
            company = scraping.driver.find_element_by_tag_name('input')
            company.send_keys(self.security)
            time.sleep(2)
            company_list = scraping.driver.find_element_by_class_name('dropdown-menu')
            company_list.click()
            scraping.driver.find_element_by_tag_name('button').click()
            time.sleep(2)

            logger.info(scraping.driver.current_url)
            script_list = scraping.driver.find_elements_by_tag_name('h4')
            script_data = map(lambda x: x.text, script_list)
            logger.info(script_data)

            scraping.driver.find_element_by_link_text('Export to Excel').click()
            time.sleep(5)
            logger.info(scraping.driver.current_url)
            logger.info('CLicked...Check')

        except Exception as e:
            logger.exception(e)

        finally:
            logger.info("******************************************************")

    def general_data(self, start_word, end_word_not_including):
        self.start_word = start_word
        self.end_word_not_including = end_word_not_including
        data = scraping.driver.find_element_by_id('content').text
        #logger.info(data)
        value = data[data.find(self.start_word):data.find(self.end_word_not_including)]
        logger.info(value)
        logger.debug(type(value))
        value = value.encode('ascii', 'ignore')
        logger.info(value)
        logger.debug(type(value))
        dataProcessing.write_to_csv(value, env.CSV_FILE)

