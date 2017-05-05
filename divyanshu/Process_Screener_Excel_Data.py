
from custom_logging import logger
from dbHandler import DBHandler
import dataProcessing
from collections import OrderedDict
import DBManager
import Module_Screener_Excel_Data
import env
import os

class Process_Screener_Excel_Data:

    def __init__(self):
        logger.info("Execeution Begins")
        logger.info("******************************")

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        #self.module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data()

        # with scraping() as sc:
        #     sc.export_to_excel(email='d.aggarwal07@yahoo.com', password='dishu@07')
        #
        #
        # dataProcessing.read_from_xlsx()#cell_range=OrderedDict([('A1','C1'),('A2','C2'),('A3','C3')]))


    def getStockNames(self):
        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_temp sn "  \
                     " where enable_for_vendor_data = 1"

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            dd["industry_vertical"] = row[3]
            data.append(dd)
        # print data
        return data

    def getStockFundamentalData(self, stock_names):

        # call module_Screener_Excel_Data for each stock
        for row in stock_names:
            nseid = row['nseid']
            dir_name = env.DOWNLOAD_DIR + nseid

            print dir_name
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)

            module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data(dir_name)
            #module_Screener_Excel_Data.getStockFundamentalData(row['nseid'])
            module_Screener_Excel_Data.readAllFilesData(nseid, dir_name)
            #module_Screener_Excel_Data.__exit__()



# if __name__ == '__main__':
#     main()
    
thisObj = Process_Screener_Excel_Data()
stock_names = thisObj.getStockNames()
thisObj.getStockFundamentalData(stock_names)
#thisObj.module_Screener_Excel_Data.getStockFundamentalData(stock_names)
#thisObj.module_Screener_Excel_Data.readAllFilesData()

