#Process_Screener_Excel_Data

from selenium import webdriver
import time
import logging
import Module_Screener_Excel_Data_OLD
import Constants as const
import platform
from selenium import webdriver
import urllib2


print __name__
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Process_Screener_Excel_Data:
    def __init__(self):
        profile = ''
        driver = ''
        self.login()
        #self.driver = self.browser

    def login(self):
        if platform.system() == 'Windows':
            self.PHANTOMJS_PATH = './phantomjs.exe'
        else:
            #self.PHANTOMJS_PATH = './phantomjs'
            self.PHANTOMJS_PATH = '/home/shopbindaas/python-workspace/phantomjs'

        self.driver = webdriver.PhantomJS(self.PHANTOMJS_PATH)

        ##############################################################
        self.driver.get('https://www.screener.in/login/')
        username = self.driver.find_element_by_name('username')
        password = self.driver.find_element_by_name('password')
        username.send_keys('amitji@gmail.com')
        password.send_keys('Welcome2020!')
        login_attempt = self.driver.find_element_by_class_name("btn-primary")
        login_attempt.click()



    def export_to_excel(self, security='INFY'):
        try:

            self.driver.get('https://www.screener.in/company/YESBANK/')
            time.sleep(5)

            logger.info('Page Source: ' + self.driver.page_source)

            element = self.driver.find_element_by_xpath("//a[contains(@href,'/excel/')]")
            href = element.get_attribute('href')

            #filename = href.rsplit('/', 1)[-1]
            self.try1(href)
            #self.try2(href)

            #urllib.urlretrieve(href, const.excel_output_excel)

            logger.info('Excel read done')

            # r = requests.get(href)
            # text = r.iter_lines()
            # reader = csv.DictReader(text, delimiter=',')
            # for line in reader:
            #     print line




            # for link in self.driver.select('a[href^="/excel/"]'):
            #     href = link.get('href')
            #     logger.info('href: ' + href)

            # script_list = self.driver.find_elements_by_tag_name('h4')
            # script_data = map(lambda x: x.text, script_list)
            # logger.info(script_data)

            # self.driver.find_element_by_link_text('Export to Excel').click()
            # time.sleep(2)
            # logger.info(self.driver.current_url)
            # logger.info('CLicked...Check')

        except Exception as e:
            logger.exception(e)

        finally:
            logger.info("******************************************************")
    def try1(self, url):

        response = urllib2.urlopen(url)
        # data = response.read()  # a `bytes` object
        # text = data.decode('utf-8')  # a `str`; this step can't be used if data is binary
        # logger.info('Text - '+ text)

        f = open(const.excel_output_excel, 'wb')
        meta = response.info()
        #file_size = int(meta.getheaders("Content-Length")[0])
        #print "Downloading: %s Bytes: %s" % (const.excel_output_excel, file_size)
        block_sz = 8192
        while True:
            buffer = response.read(block_sz)
            if not buffer:
                break
            f.write(buffer)

        f.close()

    def general_data(self, start_word, end_word_not_including):
        self.start_word = start_word
        self.end_word_not_including = end_word_not_including
        data = self.driver.find_element_by_id('content').text
        #logger.info(data)
        value = data[data.find(self.start_word):data.find(self.end_word_not_including)]
        logger.info(value)
        logger.debug(type(value))
        value = value.encode('ascii', 'ignore')
        logger.info(value)
        logger.debug(type(value))
        Module_Screener_Excel_Data_OLD.write_to_csv(value, const.excel_output_csv)

thisObj = Process_Screener_Excel_Data()
thisObj.export_to_excel()
