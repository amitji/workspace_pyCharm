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

#        select_sql = "SELECT SUBSTR(fullid, 5) nseid, short_name FROM stocksdb.fa_financial_ratio  "
#        select_sql += " union " 
#        select_sql += " SELECT  SUBSTR(fullid, 5) nseid, short_name  FROM stocksdb.fa_financial_ratio_secondary  "
        
        # sql to try on less records
#        select_sql = "SELECT SUBSTR(fullid, 5) nseid, short_name FROM stocksdb.fa_financial_ratio  "
        
        #testing 1
#        select_sql = "select  nseid, nseid as short_name from stocksdb.amit_portfolio sn where nseid in ('ICICIBANK', 'AXISBANK')"
        #testing 2 
        select_sql = "select symbol nseid , symbol as short_name from stocksdb.fo_mktlots sn "

        #Only amit_portfolio
#        select_sql = "select nseid, nseid as short_name  from stocksdb.amit_portfolio sn where sn.is_inactive='n' order by nseid desc"        

        df = pd.read_sql(select_sql, self.con)
#        df = df['nseid'].str.strip() 
        df = df.apply(lambda x: x.str.strip())
        return df  

    def getLastComment(self):

        select_sql = "select  a.nseid, a.comments from stock_sepa_analysis a, (select max(id) as id from stock_sepa_analysis where comments != ' ' group by nseid) b  "
        select_sql += " where a.id = b.id " 


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
            
#            print('Processing  - ', nseid, ' at # - ', index)
            df = self.getMarketDataForAStock(row['nseid'])
            
            if df.empty:
#                print("No Data for ", nseid)
                continue
            
            fulldf = df.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
           
            
            newdf = self.calculateAverages(fulldf) 
            #newdf has only one record now
            newdf['short_name'] = [short_name]
            newdf = self.findSEPAStage(newdf, fulldf) # you need complere df to get last 10 records for filtering volumes for up/down days
            outputDf = outputDf.append(newdf)
            
        return outputDf

    def findSEPAStage(self, mydf, fulldf):
        
        nseid = mydf['nseid'].iloc[0]
        last_x_records_df = fulldf.tail(10)
        pos_day_count = 0
        neg_day_count = 0
        pos_days_vol_sum = 1 #avoid divid by zero !
        neg_days_vol_sum = 1
        pos_close_chg_sum = 0
        neg_close_chg_sum = 0
        neg_to_pos_vol_ratio = 0
        
        for index, row in last_x_records_df.iterrows():
#            print(row)
            if(row['perct_change'] > 0 ):
                pos_day_count +=1
                pos_days_vol_sum +=row['volume']
                pos_close_chg_sum +=row['perct_change']
            else:
                neg_day_count +=1
                neg_days_vol_sum +=row['volume']
                neg_close_chg_sum +=row['perct_change']
            
        neg_to_pos_vol_ratio = round(neg_days_vol_sum/pos_days_vol_sum, 2)
        pos_close_chg_sum = round(pos_close_chg_sum,2)
        neg_close_chg_sum = round(neg_close_chg_sum,2)
        print(nseid, pos_day_count,neg_day_count,pos_days_vol_sum,neg_days_vol_sum,neg_to_pos_vol_ratio,pos_close_chg_sum, neg_close_chg_sum)
        
        #Stage  2, Stage 1->2 Logic
        if((mydf['50dma'] > mydf['100dma'] ).bool() and (mydf['100dma'] > mydf['200dma']).bool()):
#            print(" Yes - Close DMA Criteria Met for ",nseid)
            if((mydf['5d_avg_vol'] > mydf['10d_avg_vol'] ).bool() and (mydf['10d_avg_vol'] > mydf['15d_avg_vol']).bool()):
                mydf['Stage'] = ['Stage-2']
            elif((mydf['5d_avg_vol'] > mydf['10d_avg_vol'] ).bool()):
                mydf['Stage'] = ['Stage 1->2 May be']
            else:
                mydf['Stage'] = ['Stage 1 May be']
        #Stage 1->2 Logic
        elif((mydf['50dma'] < mydf['100dma'] ).bool() and (mydf['100dma'] < mydf['200dma']).bool()):
            if(((mydf['5d_avg_vol'] > mydf['10d_avg_vol'] ).bool() or (mydf['10d_avg_vol'] > mydf['15d_avg_vol']).bool() )and pos_days_vol_sum > neg_days_vol_sum  
                   and (neg_close_chg_sum+pos_close_chg_sum) > 4 and (mydf['close'] > mydf['50dma']).bool()): #and (mydf['10d_avg_vol'] > mydf['15d_avg_vol']).bool()):
                mydf['Stage'] = ['Stage 1->2']
                print("\n\n**** Stage 1->2 for ", nseid , "\n\n")
#            else:
#                print("\n\n**** Figure Out Stage for ", nseid , "\n\n")
#                mydf['Stage'] = ['Figure Out Stage']            

        
        #Stage 3->4 logic
        elif(neg_days_vol_sum > pos_days_vol_sum and (neg_close_chg_sum+pos_close_chg_sum) < -4
              and  (mydf['50dma'] < mydf['100dma']).bool() and (mydf['100dma'] > mydf['200dma']).bool() ):
            mydf['Stage'] = ['Stage 3->4 !']
            print("\n\n**** Stage 3->4 ! for ", nseid , "\n\n")
        #Stage 3        
        elif((mydf['close'] < mydf['50dma']).bool() and (mydf['50dma'] > mydf['100dma']).bool() and (mydf['100dma'] > mydf['200dma']).bool() ):
                 #and (mydf['50dma'] > mydf['100dma'] ).bool()  and (mydf['50dma'] > mydf['200dma'] ).bool() ):
            mydf['Stage'] = ['Stage 3 ']
            print("\n\n**** Stage 3 ! for ", nseid , "\n\n")
        # Amit - code for folowing logi
        #if close > 50dma and close > 100 dma and 50 dma < 100dma < 200 dma
        #then its started going up (stage 1->2)... run this and see stocks. Allcargo, TNPETRO
        
        #No Stage perhaphs...
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
        # Amit - dont drop these col, now added to SEPA table
#        df = df.drop(['prev_day_close', 'perct_change','prev_day_vol','vol_chg_perct'], axis=1)
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

    def copyCommentsFromPreviousRecords(self,df):
        
        commDf = self.getLastComment()
        df['comments'] = ''
        for i, row in commDf.iterrows():
            nseid = row['nseid']
            comments = row['comments']
            df.loc[df['nseid'] == nseid, 'comments'] = comments
            

            
        return df
       
        
thisObj = Process_SEPA_Analysis()
pd.set_option('display.expand_frame_repr', False)
np.set_printoptions(suppress=True)
pd.options.display.float_format = '{:.2f}'.format

start_time = time.time()
stock_names= thisObj.getStockList()
#print ("stock_names - \n", stock_names)
print('Processing # of stocks - ', len(stock_names))
df = thisObj.findSEPA(stock_names)

#df = df.loc[df['Stage'] != 'None']
df = df.loc[df['Stage'] == 'Stage 1->2']



print(df)
#df = df[(df['close'] < 400) & (df['dist_50dma'] > 9)]

# uncomment 2 lines for prod
#df = thisObj.copyCommentsFromPreviousRecords(df)
#thisObj.saveInDB(df)

#print ("\n\n****** Saved results in DB *****************" )
#print("Total time taken by process --- %s seconds ---" % (time.time() - start_time))
EmailUtil.send_email_with_body("Process_SEPA_Analysis.py",df)
#    