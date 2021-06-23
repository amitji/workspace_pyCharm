#Amit Purpose
# This process is same as Process_NSE_Based_ResultDates_Screener_ScrapNUpdate. 
# Just us ethis process for updating all quaterly data, Financial ratios adhoc basis.

import Module_Scrapper_Screener_India_Stocks
import Module_Final_Rating
import DBManager
import Constants


class Process_Scrapper_Screener_India_Stock:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()
        self.finalRatingModule = Module_Final_Rating.Module_Final_Rating()


    def getStockList(self):
 
        ####   USE NEW TABLE stock_names_new    ######
        #
        ##############################################
        
        ### This SQL will get all the stcoks in stock_names_new table which does not have any data in Fin Ratio / Quaterly tables..
#        select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_new sn where exchange='NSE' and fullid not in "
#        select_sql += " (select fullid from  stocksdb.fa_financial_ratio union all select fullid from  stocksdb.fa_financial_ratio_secondary )"


        #select_sql ="select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_new where exchange='NSE' and is_video_available='y' and enable_for_vendor_data='2' and id < 500 "

        # Test and Run for One Stock
        # select_sql = "select fullid, nseid,industry_vertical from stocksdb.stock_names_new sn where include_in_next_run = 'y' and nseid in ('GRAVITA') "
        
        
        #Amit Portfolio Stocks Only  - run it for all Amit's portfolio stocks
        # select_sql = "select sn.fullid, sn.nseid, sn.enable_for_vendor_data, sn.industry_vertical from stocksdb.stock_names_new sn, amit_portfolio ap where sn.nseid = ap.nseid"


        ###Explain sql -  this sql will try update all those stocks whihc does  not have latest quater results. Run this when data is not updated for long time.
        select_sql = "select fullid, nseid,industry_vertical from stocksdb.stock_names_new sn where sn.exchange ='NSE' and sn.include_in_next_run ='y' and  nseid not in "
        select_sql += "(select nseid from"
        select_sql += "(select nseid from stocksdb.fa_quaterly_data where  period = '"+Constants.latest_period+"' and quater_sequence=5 ) temp )"
        


        #run this sql to recalculate the ratings only. Comment module_Scrapper_Screener_India_Stocks.updateAll function below.
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_new sn where fullid in (select fullid from stocksdb.final_rating ) "
        
        print ('\n\nSelect SQL  - ',select_sql)
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            if dd["fullid"] is None:
                dd["fullid"] = 'NSE:'+dd["nseid"]
            # print( 'dd["fullid"] - ',dd["fullid"])           
            #dd["enable_for_vendor_data"] = row[2]
            dd["industry_vertical"] = row[2]
            data.append(dd)
        # print data
        return data


thisObj = Process_Scrapper_Screener_India_Stock()
stock_names= thisObj.getStockList()
print ("stock_names - \n", stock_names)
stock_names = thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)
if(len(stock_names) > 0):
    thisObj.finalRatingModule.updateAll(stock_names)
else:
    print (" FinalRatingModule is not run since zero stocks in GOOD list")

