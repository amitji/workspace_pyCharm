#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import Module_Scrapper_Screener_India_Stocks
import Module_Final_Rating
import DBManager

class Process_Scrapper_Screener_India_Stock:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()
        self.finalRatingModule = Module_Final_Rating.Module_Final_Rating()


    def getStockList(self):
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and enable_for_vendor_data = 'e' "
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and enable_for_vendor_data = 'z' "
        # select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='A' "
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where nseid in ('UCOBANK') "
        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_temp sn"
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


thisObj = Process_Scrapper_Screener_India_Stock()
stock_names= thisObj.getStockList()
#thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
thisObj.finalRatingModule.updateAll(stock_names)

