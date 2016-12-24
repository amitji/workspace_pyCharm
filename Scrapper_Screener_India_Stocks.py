#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import Module_Scrapper_Screener_India_Stocks
import FinalRatingModule
import DBManager

class Scrapper_Screener_India_Stocks:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()
        self.finalRatingModule = FinalRatingModule.FinalRatingModule()


    def getStockList(self):
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and enable_for_vendor_data = 'e' "
        select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and enable_for_vendor_data not in ('1', '2')"
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='A' "
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where nseid in ('SHILPAMED') "

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


thisObj = Scrapper_Screener_India_Stocks()
stock_names= thisObj.getStockList()
thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
thisObj.finalRatingModule.updateAll(stock_names)

