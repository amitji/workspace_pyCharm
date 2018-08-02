# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 06:28:00 2018

@author: amahe6
"""
import numpy as np
import pandas as pd
import DBManager
import Module_Get_Live_Data_From_Google
import time
import EmailUtil

class Process_SEPA_Analysis:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()
        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()

    def getStockList(self):

        ##Get all stocks from amit_portfolio and all FO stocks         
#        select_sql = "select symbol nseid, 'n' owned from stocksdb.fo_mktlots fo where symbol not in (select nseid from stocksdb.amit_portfolio ) "
#        select_sql += " union select nseid, 'y' owned from stocksdb.amit_portfolio sn where sn.is_inactive='n' order by nseid desc "

#        select_sql = "SELECT SUBSTR(fullid, 5) nseid FROM stocksdb.fa_financial_ratio where last_price < 80 and last_price > 60 "
#        select_sql += " union " 
#        select_sql = " SELECT  SUBSTR(fullid, 5) nseid  FROM stocksdb.fa_financial_ratio_secondary where last_price < 80 and last_price > 60 "

        select_sql = "SELECT SUBSTR(fullid, 5) nseid, short_name FROM stocksdb.fa_financial_ratio  "
        select_sql += " union " 
        select_sql += " SELECT  SUBSTR(fullid, 5) nseid, short_name  FROM stocksdb.fa_financial_ratio_secondary  "

        
        #testing
#        select_sql = "select symbol nseid, 'y' owned from stocksdb.fo_mktlots sn where symbol in ('NCC', 'ITC', 'HAVELLS')"
        
#        select_sql = "select symbol nseid from stocksdb.fo_mktlots sn "

        #Only amit_portfolio
#        select_sql = "select nseid, 'y' owned from stocksdb.amit_portfolio sn where sn.is_inactive='n' order by nseid desc"        

        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip() 
        df = df.apply(lambda x: x.str.strip())
        return df  

    def getMarketDataForAStock(self, nseid):
        
        select_sql = "select * from stocksdb.stock_market_data smd where smd.nseid = '%s' order by my_date  " % (nseid)
        df = pd.read_sql(select_sql, self.con)
        return df            
        
        
    def findSEPA(self,stock_names):
        
        outputDf = pd.DataFrame()
        for index, row in stock_names.iterrows():
            nseid = row.nseid
            short_name = row.short_name
            
            print('Processing  - ', nseid, ' at # - ', index)
            df = self.getMarketDataForAStock(row['nseid'])
            
            if df.empty:
#                print("No Data for ", nseid)
                continue
            
            df = df.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
           
            
            newdf = self.calculateAverages(df)
            newdf['short_name'] = [short_name]
            newdf = self.potentilaBuySellCall(newdf)
            outputDf = outputDf.append(newdf)
            
        return outputDf

    def potentilaBuySellCall(self, mydf):
        


#        nseid = mydf['nseid'].iloc[0]
        
        
        if((mydf['50dma'] > mydf['100dma'] ).bool() and (mydf['100dma'] > mydf['200dma']).bool()):
#            print(" Yes - Close DMA Criteria Met for ",nseid)
            if((mydf['5d_avg_vol'] > mydf['10d_avg_vol'] ).bool() and (mydf['10d_avg_vol'] > mydf['15d_avg_vol']).bool()):
                mydf['Stage'] = ['Stage-2']
            elif((mydf['5d_avg_vol'] > mydf['10d_avg_vol'] ).bool()):
                mydf['Stage'] = ['Stage1->2']
            else:
                mydf['Stage'] = ['Stage 1 May be']
        else:
#            print(" NO - Close DMA Criteria is NOT Met for ",nseid)
            mydf['Stage'] = ['None']
        
        
        return mydf


    def calculateAverages(self, df):

        df['50dma'] = df['close'].rolling(window=50).mean()
        df['100dma'] = df['close'].rolling(window=100).mean()
        df['200dma'] = df['close'].rolling(window=200).mean()
        
        df['5d_avg_vol'] = df['volume'].rolling(window=5).mean()
        df['10d_avg_vol'] = df['volume'].rolling(window=10).mean()
        df['15d_avg_vol'] = df['volume'].rolling(window=15).mean()
        
        df = df.drop(['prev_day_close', 'perct_change','prev_day_vol','vol_chg_perct'], axis=1)
#        print(df[-1:])
        #you need only last record
        df = df[-1:]
        df['dist_50dma'] = ((df['close'] - df['50dma'])* 100)/df['50dma']
        df['dist_100dma'] = ((df['close'] - df['100dma'])* 100)/df['100dma']
        
        df['5d_10d_vol_chg'] = ((df['5d_avg_vol'] - df['10d_avg_vol'])* 100)/df['10d_avg_vol']
        df['5d_15d_Vol_chg'] = ((df['5d_avg_vol'] - df['15d_avg_vol'])* 100)/df['15d_avg_vol']
        
        return df        

    
    def saveInDB(self, df):
        df.to_sql('stock_sepa_analysis', self.engine, if_exists='append', index=False)         

       
        
thisObj = Process_SEPA_Analysis()
pd.set_option('display.expand_frame_repr', False)
np.set_printoptions(suppress=True)
pd.options.display.float_format = '{:.2f}'.format

start_time = time.time()
stock_names= thisObj.getStockList()
#print ("stock_names - \n", stock_names)
print('Processing # of stocks - ', len(stock_names))
df = thisObj.findSEPA(stock_names)
df = df.loc[df['Stage'] != 'None']
print(df)
#df = df[(df['close'] < 400) & (df['dist_50dma'] > 9)]


thisObj.saveInDB(df)

#print ("\n\n****** Saved results in DB *****************" )
#print("Total time taken by process --- %s seconds ---" % (time.time() - start_time))
EmailUtil.send_email_with_body("Process_SEPA_Analysis.py",df)
#    