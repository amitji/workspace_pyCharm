
#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio table

import DBManager
import datetime

import NSELiveDataModule


class NSE_High_Low_Last_Price_Update:

    def __init__(self):
        print "Calling parent constructor"

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.qd_exception_list = []
        self.fr_exception_list = []

        # _stocks = self.getStocks()

    def getStocksMarkedForUpdates(self, table_name):


        self.cur.execute(sql)
        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            #dd["data_type"] = row[1]
            data.append(dd)
        # print data
        return data

    def updateLiveData(self, data, table_name):

        fullid = data["fullid"]
        #fullid = "NSE:" + nseid
        nseid = fullid.split("NSE:",1)[1]
        nseidModified = nseid.replace("&", "")
        nseidModified = nseid.replace("-", "_")


        try:
            #nse = Nse()
            #q = nse.get_quote(str(nseid))

            nseDict = NSELiveDataModule.getNSELiveData(nseidModified)
            high52 = nseDict['high52']
            low52 = nseDict['low52']
            #companyName = q['companyName']
            last_price = nseDict['last_price']
            #print nseid, " high 52 -",  high52
        except Exception, e3:
            print str(e3)
            print "\n******Amit - Exception in getting NSE data for - " + nseid
            self.fr_exception_list.append(nseid)
            return

        try:
            high52 = float("{0:.2f}".format(high52))
            low52 = float("{0:.2f}".format(low52))
            last_price = float("{0:.2f}".format(last_price))
            #last_price_vs_high52 = (2 - (high52/last_price) ) * 100
            last_price_vs_high52 = (last_price/high52)*100
            last_price_vs_high52 = float("{0:.2f}".format(last_price_vs_high52))
            print "last_price_vs_high52 - ",last_price_vs_high52

            now = datetime.date.today();
            update_now = 'n'
            update_sql = "update "+table_name+" set last_price='%s', 52_week_high = '%s' , 52_week_low = '%s', last_price_vs_high52='%s', update_now='%s', last_modified='%s'  where fullid = '%s'  " % (last_price,high52,low52,last_price_vs_high52, update_now, now, fullid);

            self.cur.execute(update_sql)
            print "Update executed for table  "+table_name

        except  Exception, e:
            print str(e)
            print "\n******Amit - some excetion executing upadte sql for  - " + nseid
            self.qd_exception_list.append(nseid)
            pass




    def __del__(self):
        self.cur.close()
        self.con.close()


