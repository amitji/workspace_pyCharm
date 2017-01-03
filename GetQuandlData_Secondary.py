#!/usr/bin/env python
import DBManager
import time
import QuandlDataModule
import EmailUtil

class GetQuandlData_Secondary:

    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.quandlDataObject = QuandlDataModule.QuandlDataModule()


    def getStocksEnabledForVendorData_V2(self):
        #sql = "SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '2' and update_now != 'c' order by id"
        #sql = "SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '2' order by id"

        # run for a specific stock
        #sql = "SELECT distinct nseid, data_type FROM stock_names where  update_now = 'A' order by id"
        # run for error stocks
        sql = "SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = 'e' or enable_for_vendor_data = 'z'  order by id"

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
        self.quandlDataObject.updateQuarterlyData(row, 'fa_quaterly_data_secondary')

    def updateFinancialRatios(self, row):
        self.quandlDataObject.updateFinancialRatios(row, 'fa_financial_ratio_secondary')


    def __del__(self):
        self.cur.close()
        self.con.close()




thisObj = GetQuandlData_Secondary()
start_time = time.time()
stock_names= thisObj.getStocksEnabledForVendorData_V2()
print "Number of Stocks processing - " , len(stock_names)
print stock_names
totalCount = len(stock_names)
count = 0
#
for row in stock_names:
    # print row
    count = count+1
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

url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess_Secondary"
print "Now run the URL ", url
EmailUtil.send_email("GetQuandlData_Secondary", thisObj.quandlDataObject.updated_stock_list,  url)
