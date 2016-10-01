#!/usr/bin/env python
import DBManager
import time
import QuandlDataModule
import EmailUtil

class GetQuandlData:

    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.quandlDataObject = QuandlDataModule.QuandlDataModule()

    def getStocksEnabledForVendorData(self):
        sql = "SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '1' and update_now = 'y' order by id"
        #sql = "SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '1' order by id"

        # Use below sql when quartly table has no data for a stock
        #sql = "SELECT distinct nseid, data_type FROM stocksdb.stock_names where enable_for_vendor_data='1' and fullid not in (select fullid from fa_quaterly_data)"
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["nseid"] = row[0]
            dd["data_type"] = row[1]
            data.append(dd)
        # print data
        return data

    def updateQuarterlyData(self, row):
        self.quandlDataObject.updateQuarterlyData(row, 'fa_quaterly_data')

    def updateFinancialRatios(self, row):
        self.quandlDataObject.updateFinancialRatios(row, 'fa_financial_ratio')


    def __del__(self):
        self.cur.close()
        self.con.close()



thisObj = GetQuandlData()
start_time = time.time()
stock_names= thisObj.getStocksEnabledForVendorData()
print stock_names
print "Number of Stocks processing - " , len(stock_names)
totalCount = len(stock_names)
count = 0
#
for row in stock_names:
    # print row
    count = count + 1
    print "\n\ncalling updateQuaterlyData for - ", row['nseid'], "(", count, "/", totalCount, ")"
    thisObj.updateQuarterlyData(row)
    thisObj.updateFinancialRatios(row)

print "\n\n Quarterly Data Exception list - "
print thisObj.quandlDataObject.qd_exception_list
print "\nFinancial Ratio Exception list - "
print thisObj.quandlDataObject.fr_exception_list

#print "\n NSE Api Exception list - "
#print thisObj.nse_exception_list
print "\n Updated Stock list for - ", len(thisObj.quandlDataObject.updated_stock_list), " Stocks"
print thisObj.quandlDataObject.updated_stock_list

print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )
url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
print "Now run the URL ", url
EmailUtil.send_email("GetQuandlData Process", thisObj.quandlDataObject.updated_stock_list, url)