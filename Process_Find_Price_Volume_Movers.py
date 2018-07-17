##Amit - purpose - Find our Harami pattern (Bullisj and bearish)


"""
Created on Mon Jul  9 14:22:47 2018

@author: amimahes
"""


import csv
import DBManager
import platform
import requests
import datetime
import pandas as pd

import Constants
import EmailUtil


class Process_Find_Price_Volume_Movers:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.bullish_harami_list = []
        self.bearish_harami_list = []


    def getStockList(self ):

            
        select_sql = "select * from stocksdb.stock_market_data_bkup where date(my_date) > date(now()- INTERVAL 2 DAY) "
        select_sql += " and perct_change > 4 order by perct_change desc "
  

        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip()
        df = df.apply(lambda x: x.str.strip())
        return df 

    def findHaramiPattern(self, stock_names):
#        print("stock_details  - " ,stock_details )
        outputDf = pd.DataFrame()
        for index, row in stock_names.iterrows():
            nseid = row.nseid
            df = self.getMarketDataForAStock(row['nseid'])
#            print(df)
#            df = df.reset_index(drop=True)
            
            if df.empty:
#                print("No Data for ", nseid)
                continue
            #R1 is last record and R2 is previous to that( a day previous)
            R1 = df.iloc[[0]].reset_index(drop=True)
            R2 = df.iloc[[1]].reset_index(drop=True)
            if ((R1.open < R1.close) & (R2.close < R2.open) & (R1.open > R2.close) & (R1.close < R2.open) & (R1.high < R2.open ) & (R1.low > R2.close)).bool():
                print("*********** Found Bullish  Harami for - " , nseid)
                self.bullish_harami_list.append(nseid)
#            if ((R1.open > R1.close) & (R2.close > R2.open) & (R1.open < R2.close) & (R1.close > R2.open)  & (R1.high < R2.close ) & (R1.low > R2.open)).bool():
            elif ((R1.open > R1.close) & (R2.close > R2.open) & (R1.open < R2.close) & (R1.close > R2.open)  & (R1.high < R2.close ) & (R1.low > R2.open)).bool():
                print("*********** Found  Bearish Harami for - " , nseid)
                self.bearish_harami_list.append(nseid)
            else:
                print("NOT found Harami for - " , nseid)
                
            
#            print ()
        
    def getMarketDataForAStock(self, nseid):
        
        select_sql = "select  * from stocksdb.stock_market_data smd where smd.nseid = '%s' order by my_date desc limit 2 " % (nseid)
        df = pd.read_sql(select_sql, self.con)
        return df  
# ----------------------------------------------------------------------

thisObj = Process_Find_Price_Volume_Movers()


stock_names = thisObj.getStockList()
#stock_details = thisObj.getMarketDataForAStock(stock_names)
thisObj.findHaramiPattern(stock_names )
print ( "Bullish Harami Stocks - ", thisObj.bullish_harami_list)
print ( "Bearish Harami Stocks - ", thisObj.bearish_harami_list)
EmailUtil.send_email_as_text("Process_find_pattern_Harami",  thisObj.bullish_harami_list, thisObj.bearish_harami_list)


