
#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio table

import DBManager
import datetime

import NSELiveDataModule


class NSE_High_Low_Last_Price_Update:

    def __init__(self):
        print ("Calling parent constructor")

        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()
        self.qd_exception_list = []
        self.fr_exception_list = []

        # _stocks = self.getStocks()

    def getStocksMarkedForUpdates(self, table_name):

        sql = "SELECT distinct fullid FROM "+table_name+"  order by fullid "
        #sql = "SELECT distinct fullid FROM " + table_name + " where fullid like '%-%'  order by fullid "

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
        nseidModified = nseidModified.replace("-", "_")


        try:
            #nse = Nse()
            #q = nse.get_quote(str(nseid))

            mydata = NSELiveDataModule.getNSELiveData(nseidModified)
            nseDict = NSELiveDataModule.getHighLowClose(mydata)
            
            #Amit- save data as needed
#            self.saveAllInDB(mydata,nseidModified)
            self.saveLastRecordInDB(mydata,nseidModified)
            
    
            high52 = nseDict['high52']
            low52 = nseDict['low52']
            #companyName = q['companyName']
            last_price = nseDict['last_price']
            #print nseid, " high 52 -",  high52
        except Exception as e3:
            print ("OS error: {0}".format(e3))
            print ("\n******Amit - Exception in getting NSE data for - " + nseid)
            self.fr_exception_list.append(nseid)
            return

        try:
            high52 = float("{0:.2f}".format(high52))
            low52 = float("{0:.2f}".format(low52))
            last_price = float("{0:.2f}".format(last_price))
            #last_price_vs_high52 = (2 - (high52/last_price) ) * 100
            last_price_vs_high52 = (last_price/high52)*100
            last_price_vs_high52 = float("{0:.2f}".format(last_price_vs_high52))
            print ( "last_price_vs_high52 - ",last_price_vs_high52)

            now = datetime.date.today();
            update_now = 'n'
            update_sql = "update "+table_name+" set last_price='%s', 52_week_high = '%s' , 52_week_low = '%s', last_price_vs_high52='%s', update_now='%s', last_modified='%s'  where fullid = '%s'  " % (last_price,high52,low52,last_price_vs_high52, update_now, now, fullid);

            self.cur.execute(update_sql)
            print ( "Update executed for table  "+table_name)

        except  (Exception, e):
            print (str(e))
            print ("\n******Amit - some excetion executing upadte sql for  - " + nseid)
            self.qd_exception_list.append(nseid)
            pass



    def saveAllInDB(self,df,nseid):
    
        df.columns = ['open', 'high', 'low','last', 'close', 'volume', 'turnover']
        df['nseid'] = nseid
        df['my_date'] = df.index
        #Amit - save all 250 records in database...but first delete else it will duplicate
        delete_sql = "delete from stock_market_data where nseid='%s' " % (nseid);    
        self.cur.execute(delete_sql)
        self.con.commit()       
        df.to_sql('stock_market_data', self.engine, if_exists='append', index=False)    
        print("saveAllInDB done for ", nseid)
        
    def saveLastRecordInDB(self,df,nseid):
        
        df.columns = ['open', 'high', 'low','last', 'close', 'volume', 'turnover']
        df['nseid'] = nseid
        df['my_date'] = df.index
        #Amit - save only last record in database...
#        df_latest = df.iloc[0]   
        df_latest = df[:1]   
        df_latest.to_sql('stock_market_data', self.engine, if_exists='append', index=False) 
        print("saveLastRecordInDB done for ", nseid)

    def __del__(self):
        self.cur.close()
        self.con.close()


