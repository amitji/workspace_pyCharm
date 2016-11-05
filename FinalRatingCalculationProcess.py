from decimal import Decimal

import DBManager
import FinalRatingModule


class FinalRatingCalculationProcess:
    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.finalRatingModule = FinalRatingModule.FinalRatingModule()


    def getStockList(self):
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and is_video_available='y' "
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where nseid in ('8KMILES') "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            data.append(dd)
        return data


thisObj = FinalRatingCalculationProcess()
stock_names= thisObj.getStockList()
thisObj.finalRatingModule.updateAll(stock_names)

