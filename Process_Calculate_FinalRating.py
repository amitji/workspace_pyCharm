from decimal import Decimal

import DBManager
import Module_Final_Rating


class Process_Calculate_FinalRating:
    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.finalRatingModule = Module_Final_Rating.Module_Final_Rating()

    # def __del__(self):
    #     # self.con.close
    #     # self.cur.close()
    #     print "\n\n*****  deleting Process_Calculate_FinalRating  "

    def getStockList(self):
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and is_video_available='y' "
        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names sn where nseid in ('ICICIBANK', 'HDFCBANK','LAKSHVILAS') "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            d["industry_vertical"] = row[3]
            data.append(dd)
        return data


thisObj = Process_Calculate_FinalRating()
stock_names= thisObj.getStockList()
thisObj.finalRatingModule.updateAll(stock_names)
#thisObj.finalRatingModule.calibrateAllRatings()
