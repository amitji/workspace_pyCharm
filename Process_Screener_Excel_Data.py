

#from custom_logging import logger
#from dbHandler import DBHandler
#import dataProcessing
#from collections import OrderedDict
import DBManager
import Module_Screener_Excel_Data
import Module_Choose_Consolidated_Or_Standalone
import env
import os
import shutil
import datetime
import time
import EmailUtil
import Constants


class Process_Screener_Excel_Data:

    def __init__(self):

        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.module_Choose_Consolidated_Or_Standalone = Module_Choose_Consolidated_Or_Standalone.Module_Choose_Consolidated_Or_Standalone()
        #self.module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data()



    def getStockNames(self):
        #select_sql = "select fullid, nseid, enable_for_vendor_data,industry_vertical from stocksdb.stock_names_temp sn "  \
         #            " where enable_for_vendor_data = 1"

        #Working sql        
#        select_sql = "select fullid, nseid from stocksdb.stock_names_for_forecasting where update_now='y' "
        
        # This sql will get the nseid which has latest results announced but not yet scrapped for Excek data.
        select_sql = "select distinct fullid, nseid from stocksdb.stock_forecasting_pe_eps_past_years_data sf where "
        select_sql += " nseid not in (select nseid from stocksdb.stock_forecasting_pe_eps_past_years_data where seq_no = 10 and fin_year='"+Constants.latest_period+"') and nseid in "
        select_sql += " (select nseid from"
        select_sql += " (select nseid from stocksdb.fa_quaterly_data_secondary where  period = '"+Constants.latest_period+"' and quater_sequence=5 "
        select_sql += " union "
        select_sql += " select nseid from stocksdb.fa_quaterly_data where period = '"+Constants.latest_period+"' and quater_sequence=5 ) temp ) " 

        #testing sql
#        select_sql = "select fullid, nseid  from stocksdb.stock_names_for_forecasting where nseid='NCC' "
        
        print("select_sql - ", select_sql)
        self.cur.execute(select_sql)
        rows = self.cur.fetchall()
        data = []
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            #dd["enable_for_vendor_data"] = row[2]
            #dd["industry_vertical"] = row[3]
            data.append(dd)
        # print data
        return data

    def getStockFundamentalData(self, stock_names):

        # call module_Screener_Excel_Data for each stock
        count = 1;
        size = stock_names.__len__();
#        module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data()
        
        for row in stock_names:
            nseid = row['nseid']
            try:

                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print ("\n****************************************************************************************")
                print("Starting for ",nseid," , processing  - ",count, " /",size, "  Time - ", now)
                count = count+1

                dir_name = env.DOWNLOAD_DIR + nseid

                print( "dir_name - ", dir_name)
                if os.path.exists(dir_name):
                    shutil.rmtree(dir_name)
                    #os.removedirs(dir_name)
                os.makedirs(dir_name)

                #first decide if you can pikc consolidated numbers or standalone

                type = self.module_Choose_Consolidated_Or_Standalone.chooseConsolidatedOrStandalone(nseid)
                print ("Type if C or S  - ", type)
                
                
                module_Screener_Excel_Data = Module_Screener_Excel_Data.Module_Screener_Excel_Data(dir_name)
                
                
                module_Screener_Excel_Data.setDirName(dir_name)
                module_Screener_Excel_Data.getStockFundamentalData(row['nseid'], type)
                module_Screener_Excel_Data.readAllFilesData(nseid,row['fullid'], dir_name, type)
                module_Screener_Excel_Data.__exit__()
                module_Screener_Excel_Data.updateFlag(row['fullid'])
            except  Exception as e:
                print ("error e - ", str(e))
                print ("\n******Amit - some excetion executing " + nseid)
                time.sleep(120) #if there is a DB connection or Internet Access issues , wait for 2 minutes
                pass

        
# if __name__ == '__main__':
#     main()
    
thisObj = Process_Screener_Excel_Data()

stock_names = thisObj.getStockNames()
if not stock_names:  #if empty
    print("Stock_names list is empty, No stocks to run the process - ")
else:
    thisObj.getStockFundamentalData(stock_names)
    

# Run the Java process from eclipse , hit url -  http://localhost:8080/StockCircuitServer/spring/stockcircuit/getStockForecastData
#msg = "Now Run the Java process from eclipse , hit url -  http://localhost:8080/StockCircuitServer/spring/stockcircuit/getStockForecastData"
#print("\n\n", msg)

#EmailUtil.send_email_as_text("Process Screener Excel Data Done", msg, "")





#thisObj.module_Screener_Excel_Data.getStockFundamentalData(stock_names)
#thisObj.module_Screener_Excel_Data.readAllFilesData()

