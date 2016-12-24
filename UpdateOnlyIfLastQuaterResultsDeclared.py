#!/usr/bin/env python
#Amit Purpose - This process will check whihc all stocks does not have the latest Quarter results till now and see if the results
# are announced and only update of latest quarter data is announced.
import quandl
import DBManager
from pandas import pandas
import time

import QuandlDataModule
import Constants
import EmailUtil


class UpdateOnlyIfLastQuaterResultsDeclared:
    # cur = ""


    def __init__(self):
        # print "Calling parent constructor"

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.changedTypeToS = False;
        self.quandlDataObject = QuandlDataModule.QuandlDataModule()



    def getStockList(self):

        select_sql = "select * from (SELECT sn.nseid, sn.data_type FROM stocksdb.fa_quaterly_data qd, stocksdb.stock_names sn where qd.quater_sequence = 5 " \
                     " and (qd.quater_name = '"+Constants.previous_quarter+"' or qd.quater_name = '"+Constants.prev_to_previous_quarter+"' ) "
        select_sql += " and qd.fullid=sn.fullid "
        select_sql += "union SELECT sn.nseid, sn.data_type FROM stocksdb.stock_names sn where enable_for_vendor_data ='1' and update_now = 'y' "
        select_sql += ") sn order by nseid"

        print select_sql
        #uncomment this when need to run for a specific Stock...
        # select_sql = " SELECT sn.nseid, sn.data_type FROM stocksdb.stock_names sn where enable_for_vendor_data ='1' and update_now = 'A' "
        self.cur.execute(select_sql)

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

        isLatestDataAvailable = False
        nseid = row["nseid"]
        type = row["data_type"]
        # changedTypeToS flag is used in case dataset type changed from Consolidated to Standalone, this needs to be passed to next function which has data type from ealry query
        self.changedTypeToS = False
        fullid = "NSE:"+nseid
        print fullid


        nseidModified = nseid.replace("&", "")
        rev_dataset = Constants.Vendor_Name+"/"+nseidModified+Constants.revenue_dataset_suffix
        #profit_dataset = Constants.Vendor_Name + "/" + nseidModified + Constants.profit_dataset_suffix

        try:
            revData = quandl.get(rev_dataset, authtoken=Constants.Authkey, rows=5, sort_order="desc")
            #profitData = quandl.get(profit_dataset , authtoken=Constants.Authkey, rows=5, sort_order="desc")

            rdf = pandas.DataFrame(revData)
            #pdf = pandas.DataFrame(profitData)

            #print rdf
            print rdf.index[0]

            if Constants.latest_quarter_date == str(rdf.index[0]):
                print "Latest results available...so moving on to process"
                isLatestDataAvailable = True

            else:
                print "Latest Quarter data is still not available for ", fullid, " so not updating anything."

                return isLatestDataAvailable

            #quandlDataObject = QuandlDataModule.QuandlDataModule()
            self.quandlDataObject.updateQuarterlyData(row, 'fa_quaterly_data')
            #self.updated_stock_list.append(fullid)
            return isLatestDataAvailable

        except  Exception, e2:
            print "error e2 - ",str(e2)
            print "\n******Amit UpdateOnlyIfLastQuaterResultsDeclared - some excetion executing fa_quaterly_data insert sql 222 for  - " + nseid
            #self.qd_exception_list.append(nseid)
            #self.all_good_flag = False
            pass


    def updateFinancialRatios(self, row):

        self.quandlDataObject.updateFinancialRatios(row,'fa_financial_ratio')



    def __del__(self):
        self.cur.close()
        self.con.close()



start_time = time.time()
thisObj = UpdateOnlyIfLastQuaterResultsDeclared()

stock_names= thisObj.getStockList()
print stock_names
print "Number of Stocks processing - " , len(stock_names)
totalCount = len(stock_names)
count = 0
#
for row in stock_names:
    # print row
    count = count + 1
    print "\n\ncalling updateQuaterlyData for - ", row['nseid'], "(", count, "/", totalCount, ")"

    isLatestDataAvailable = thisObj.updateQuarterlyData(row)
    if(isLatestDataAvailable):
        thisObj.updateFinancialRatios(row)

print "\n\n Quarterly Data Exception list - "
print thisObj.quandlDataObject.qd_exception_list
print "\nFinancial Ratio Exception list - "
print thisObj.quandlDataObject.fr_exception_list

print "\n Updated Stock list for - ", len(thisObj.quandlDataObject.updated_stock_list), " Stocks"
print thisObj.quandlDataObject.updated_stock_list

print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )

url = "http://localhost:8080/StockCircuitServer/spring/stockcircuit/calculateFADataPostPythonProcess"
print "Now run the URL ", url
EmailUtil.send_email("UpdateOnlyIfLastQuaterResultsDeclared", thisObj.quandlDataObject.updated_stock_list, url)
