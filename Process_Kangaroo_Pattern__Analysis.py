# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 06:28:00 2018

@author: amahe6
"""
import numpy as np
import pandas as pd
import DBManager
import time
import EmailUtil
import ModuleAmitException
import time as t

class Process_Kangaroo_Pattern__Analysis:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()

    def getLastComment(self):

        select_sql = "select  a.nseid, a.is_imp, a.comments from stock_sepa_analysis a, (select max(id) as id from stock_sepa_analysis where comments != ' ' group by nseid) b  "
        select_sql += " where a.id = b.id " 


        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip() 
#        df = df.apply(lambda x: x.str.strip())
        return df  
    
    def getMarketDataForAStock(self, nseid):
        
        select_sql = "select * from stocksdb.stock_market_data smd where smd.nseid = '%s' order by my_date  " % (nseid)
        df = pd.read_sql(select_sql, self.con)
        return df            
        
        
    def findKangarooPattern(self,stock_names):
        
        outputDf = pd.DataFrame()
        for index, row in stock_names.iterrows():
            short_name = row.short_name
            isFO = row.isFO
            
#            print('Processing  - ', nseid, ' at # - ', index)
            fulldf = self.getMarketDataForAStock(row['nseid'])
            
            if fulldf.empty:
#                print("No Data for ", nseid)
                continue
            
            
            MAdf = self.calculateAverages(fulldf) 
            #MAdf has only one record now
            MAdf['short_name'] = [short_name]
            MAdf['isFO'] = [isFO]

            #Amit- check for Kangaroo tail
            MAdf = self.checkForKangarooTailPattern(MAdf,fulldf);
            MAdf = MAdf.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
            outputDf = outputDf.append(MAdf)
            
        return outputDf

    def getFOStockList(self):

        #Prod sql 
        select_sql = "select symbol nseid , symbol as short_name from stocksdb.fo_mktlots sn "
        df = pd.read_sql(select_sql, self.con)
        df = df.apply(lambda x: x.str.strip())
        return df  

    def getAllStockList(self):
        # sql to run for all NSE stocks
        select_sql = "select distinct nseid, nseid as short_name, 'n' as isFO from stocksdb.stock_market_data " 
        
        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip() 
        df = df.apply(lambda x: x.str.strip())
        return df  
    
    def getStockList(self):

        ##Get all stocks from amit_portfolio and all FO stocks         

        select_sql = "select symbol as nseid, symbol as short_name, 'y' as isFO from stocksdb.fo_mktlots fo where  not exists (select nseid from stocksdb.amit_portfolio ap where ap.nseid = fo.symbol ) "
        select_sql += " union "
        select_sql += " select nseid, name as short_name, 'n' as isFO from stocksdb.amit_portfolio ap where ap.is_inactive='n' order by nseid  "

        
        # sql to run for all NSE stocks
#        select_sql = "select distinct nseid, nseid as short_name, 'n' as isFO from stocksdb.stock_market_data " 
        
        #testing 1
#        select_sql = "select nseid, nseid as short_name,'n' as isFO from stocksdb.stock_names sn where nseid in ('AXISBANK')"

#        select_sql = "select  nseid, nseid as short_name,'n' as isFO from stocksdb.amit_portfolio sn where nseid in ('APOLLOTYRE')"
        #testing 2 
#        select_sql = "select symbol nseid , symbol as short_name, 'y' as isFO  from stocksdb.fo_mktlots sn "


        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip() 
        df = df.apply(lambda x: x.str.strip())
        return df  
    
    def checkForKangarooTailPattern(self, MAdf, fulldf):
        fulldf.is_copy = False
        
        last_record = fulldf.iloc[-1:]
        second_last_record = fulldf.iloc[-2:]
        
        nseid = last_record['nseid'].values[0]
        my_date = last_record['my_date'].values[0]
        prev_high = second_last_record['high'].values[0]
        prev_low = second_last_record['low'].values[0]
        today_open = last_record['open'].values[0]
        today_close = last_record['close'].values[0]
        today_high = last_record['high'].values[0]
        today_low = last_record['low'].values[0]
        max= today_low+(today_high-today_low)/3
        min = today_high - (today_high-today_low)/3
        
#        print(max, min, today_open, today_close, prev_high, prev_low)
        #Bearish  case
        if((today_open < max) and (today_close < max) and
               (today_open < prev_high) and (today_close < prev_high) and 
               (today_open > prev_low) and (today_close > prev_low) ):
            print("Bearish Kangaroo Tail for  - ", nseid, ' - ', my_date)
            MAdf['Stage'] = ['Bearish Kangaroo']
            
            

        elif((today_open > min) and (today_close > min) and
               (today_open < prev_high) and (today_close < prev_high) and 
               (today_open > prev_low) and (today_close > prev_low) ):
            print("Bullish Kangaroo Tail for  - ", nseid, ' - ', my_date)
            MAdf['Stage'] = ['Bullish Kangaroo']
            
        else:
#           print("NO Kangaroo Tail for  - ", nseid) 
           MAdf['Stage'] = ['None']
           
        return MAdf           
           


    def calculateAverages(self, df):
        #this is to supress the copy warning..
        df.is_copy = False
        
        df['50dma'] = df['close'].rolling(window=50).mean()
        df['100dma'] = df['close'].rolling(window=100).mean()
        df['200dma'] = df['close'].rolling(window=200).mean()
        
        df['5d_avg_vol'] = df['volume'].rolling(window=5).mean()
        df['10d_avg_vol'] = df['volume'].rolling(window=10).mean()
        df['15d_avg_vol'] = df['volume'].rolling(window=15).mean()
        
        
        # Amit - dont drop these col, now added to SEPA table
#        df = df.drop(['prev_day_close', 'perct_change','prev_day_vol','vol_chg_perct'], axis=1)
#        print(df[-1:])
        #you need only last record
        df = df[-1:]
        df.is_copy = False
        df['dist_50dma'] = ((df['close'] - df['50dma'])* 100)/df['50dma']
        df['dist_100dma'] = ((df['close'] - df['100dma'])* 100)/df['100dma']
        
        df['5d_10d_vol_chg'] = ((df['5d_avg_vol'] - df['10d_avg_vol'])* 100)/df['10d_avg_vol']
        df['5d_15d_Vol_chg'] = ((df['5d_avg_vol'] - df['15d_avg_vol'])* 100)/df['15d_avg_vol']
        
        return df        

    
    def saveInDB(self, df):
        df = self.copyCommentsFromPreviousRecords(df)
        df.to_sql('stock_sepa_analysis', self.engine, if_exists='append', index=False)
        print("SEPA saved in DB")         

    def copyCommentsFromPreviousRecords(self,df):
        
        commDf = self.getLastComment()
        df['comments'] = ''
        df['is_imp'] = ''
        for i, row in commDf.iterrows():
            nseid = row['nseid']
            comments = row['comments']
            is_imp = row['is_imp']
            df.loc[df['nseid'] == nseid, 'comments'] = comments
            df.loc[df['nseid'] == nseid, 'is_imp'] = is_imp

        return df
       
        
thisObj = Process_Kangaroo_Pattern__Analysis()
pd.set_option('display.expand_frame_repr', False)
np.set_printoptions(suppress=True)
pd.options.display.float_format = '{:.2f}'.format
start_time = time.time()

run_for_all_stocks = 1

if run_for_all_stocks == 1:
    stock_names= thisObj.getAllStockList()
else:
    stock_names= thisObj.getStockList()
    
    
#fo_stocks= thisObj.getFOStockList()
print ("stock_names - \n", stock_names)
print('Processing # of stocks - ', len(stock_names))
df = thisObj.findKangarooPattern(stock_names)

if not df.empty:
    df = df.loc[df['Stage'] != 'None']
    bulldf = df.loc[df['Stage'] == 'Bullish Kangaroo']
    bearishdf = df.loc[df['Stage'] == 'Bearish Kangaroo']
    
    bulldf = bulldf[['nseid','Stage']]
    print("\n # of bULLISH stocks - ", len(bulldf))
    print(bulldf.to_string())
    

    bearishdf = bearishdf[['nseid','Stage']]
    print("\n # of bEARISH stocks - ", len(bearishdf))
    print(bearishdf.to_string())
    

          
    #df = df[(df['close'] < 400) & (df['dist_50dma'] > 9)]
    try:
        # uncomment folowing lines for prod
#        thisObj.saveInDB(df)
        print(" NOT Saved in DB")
    except Exception as e1:
        print ("\n******Exception in saving SEPA in DB, sleep for 5 minute and try...\n\n\n" )
        print (str(e1))
        ModuleAmitException.printInfo()
        #May be Db conn has gone bad so close it and initialize again.. Lets try !
        thisObj.con.close()                    
        thisObj.con = DBManager.connectDB()
        thisObj.cur = thisObj.con.cursor()
    
        t.sleep(300)
        thisObj.saveInDB(df)


#print ("\n\n****** Saved results in DB *****************" )
#print("Total time taken by process --- %s seconds ---" % (time.time() - start_time))
EmailUtil.send_email_with_body("Process_Kangaroo_Pattern__Analysis.py",bulldf.to_string()+"\n\n"+ bearishdf.to_string())
#    