
from custom_logging import logger
from dbHandler import DBHandler
import dataProcessing
from collections import OrderedDict
import DBManager
import Module_Screener_Excel_Data

class Process_Screener_Excel_Data:

    def __init__(self):
        logger.info("Execeution Begins")
        logger.info("******************************")

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data()

        # with scraping() as sc:
        #     sc.export_to_excel(email='d.aggarwal07@yahoo.com', password='dishu@07')
        #
        #
        # dataProcessing.read_from_xlsx()#cell_range=OrderedDict([('A1','C1'),('A2','C2'),('A3','C3')]))


    def getStockNames(self):
        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_temp sn "

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

# if __name__ == '__main__':
#     main()
    
thisObj = Process_Screener_Excel_Data()
stock_names = thisObj.getStockNames()
thisObj.module_Screener_Excel_Data.getStockFundamentalData(stock_names)

