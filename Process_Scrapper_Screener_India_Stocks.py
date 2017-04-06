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
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_temp sn"

        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where nseid in ('JKCEMENT', 'KALINDEE', 'KSERASERA', 'NEYVELILIG', 'OMNITECH', 'PRICOL', 'SHRENUJ', 'SICAL',  'UBHOLDINGS', 'EQUITAS', 'UJJIVAN', 'TEAMLEASE', 'PRECAM', 'RBLBANK') "
        select_sql ="select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names where exchange='NSE' and is_video_available='y' and enable_for_vendor_data='2' and id < 500 "

        #run it for all Amit's portfolio stocks
        #select_sql = "select sn.fullid, sn.nseid, sn.enable_for_vendor_data, sn.industry_vertical from stocksdb.stock_names sn, amit_portfolio ap where sn.nseid = ap.nseid"

        #run this sql to recalculate the ratings only. Comment module_Scrapper_Screener_India_Stocks.updateAll function below.
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where fullid in (select fullid from stocksdb.final_rating ) "
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
all_good_stock_names = thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
if(len(all_good_stock_names) > 0):
    thisObj.finalRatingModule.updateAll(all_good_stock_names)
else:
    print " FinalRatingModule is not run since zero stocks in GOOD list"

