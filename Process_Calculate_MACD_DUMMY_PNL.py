# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 06:28:00 2018

@author: amahe6
"""
import pandas as pd
import DBManager
import Module_Get_Live_Data_From_Google
import datetime as dt
import time

class Process_Calculate_MACD_DUMMY_PNL:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()
        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()


    def getMacdRSIData(self):
            
        select_sql = "select * from stocksdb.stock_macd_rsi_analysis sn where date(my_date) > date(now()- INTERVAL 5 DAY) "
#        select_sql = "select * from stocksdb.stock_macd_rsi_analysis sn where nseid in ('SUZLON','BHUSANSTL') "
        
        df = pd.read_sql(select_sql, self.con)
        return df  
            


    def saveInDB(self, liveQuotes,macd_rsi_data):
        newDf = pd.DataFrame()
        for i, row in macd_rsi_data.iterrows():
            nseid = row.nseid
            
            data = liveQuotes.loc[liveQuotes['nseid'] == nseid]
            print('Data - ', data)
            last_price = data.get('l')
            print ("last_price - ",last_price)
            if last_price is not None :
                last_price = float(data.get('l'))
                close = row.close
                row['profit_loss'] = round((last_price-close)*100/close, 2)
#                row['time_now'] = [pd.to_datetime('now')+pd.Timedelta('05:30:00')]
                row['time_now'] = dt.datetime.now()
                row['price_now'] = last_price    
                row['lot_size'] = 100
                
                
                #delete existing row before we finally insert
#                id = row.id
                delete_sql = "delete from stocksdb.stock_macd_rsi_analysis where id = %s " %(row.id)
                self.cur.execute(delete_sql)
                
                newDf = newDf.append(row)  
                
                
                                    
        self.con.commit()                            
        #now save in DB   
        print ('NewDF to be saved- \n', newDf)     
        newDf = newDf.drop(['id'],1)
        newDf.to_sql('stock_macd_rsi_analysis', self.engine, if_exists='append', index=False)         

          

     
        
thisObj = Process_Calculate_MACD_DUMMY_PNL()
start_time = time.time()
macd_rsi_data = thisObj.getMacdRSIData()
names = macd_rsi_data.nseid.unique()
stock_names = pd.DataFrame(names, columns=['nseid'])

liveQuotes = pd.DataFrame(thisObj.module_Get_Live_Data_From_Google.getLiveQuotesForMultipleStock(stock_names))
print('liveQuotes - ', liveQuotes)

thisObj.saveInDB(liveQuotes,macd_rsi_data )

print ("Saved results in DB" )
print("--- %s seconds ---" % (time.time() - start_time))

    