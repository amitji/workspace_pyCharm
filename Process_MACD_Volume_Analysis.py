# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 06:28:00 2018

@author: amahe6
"""
import pandas as pd
import DBManager
import Module_Get_Live_Data_From_Google

class Process_MACD_Volume_Analysis:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()

        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()
#        self.finalRatingModule = Module_Final_Rating.Module_Final_Rating()


    def getStockList(self):
 
#        select_sql = "select fullid, nseid from stocksdb.stock_names sn where exchange='NSE' "
#        select_sql = "select nseid from stocksdb.amit_portfolio sn "
#        select_sql = "select symbol nseid from stocksdb.fo_mktlots sn "
        select_sql = "select fullid, nseid from stocksdb.stock_names sn where nseid in ('SUZLON') "

        df = pd.read_sql(select_sql, self.con)

        return df  

    def getMarketDataForAStock(self, nseid):
        
        select_sql = "select * from stocksdb.stock_market_data smd where smd.nseid = '%s' order by my_date  " % (nseid)

        df = pd.read_sql(select_sql, self.con)

        return df            
        
        
    def calculateMACDAndRSI(self,stock_names):
        outputDf = pd.DataFrame()
        for index, row in stock_names.iterrows():
            nseid = row['nseid']
            df = self.getMarketDataForAStock(row['nseid'])
            
            if df.empty:
#                print("No Data for ", nseid)
                continue
            
            df = df.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
#            print (df)
            rsiDf =   self.calculateRSI(df)
            
            newdf = self.MACD(df)
            newdf['nseid'] = nseid
#            newdf.to_csv("temp_mcad_output-2.csv")
#            temdf = self.makeBuySellCall(newdf)
#            outputDf = outputDf.append(temdf)
            returnDf = self.potentilaBuySellCall(newdf, rsiDf)
            outputDf = outputDf.append(returnDf)
         
            
  
        return outputDf    
            
    def makeBuySellCall(self, df):
        
        mydf = df[-2:]
        nseid = mydf['nseid'].iloc[1]
        prev_close = mydf['close'].iloc[1]
        returnDf = pd.DataFrame()
        returnDf['nseid'] = [nseid]
        returnDf['prev_close'] = [prev_close]
        returnDf['my_date'] = [pd.to_datetime('today')]
        
        listLongShort = []    # Since you need at least two days in the for loop

        if mydf['MACD'].iloc[1] > mydf['macd_9ema_signal'].iloc[1] and mydf['MACD'].iloc[0] <= mydf['macd_9ema_signal'].iloc[0]:
            listLongShort.append("BUY")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['prediction'] = ['BUY']             
        elif mydf['MACD'].iloc[1] < mydf['macd_9ema_signal'].iloc[1] and mydf['MACD'].iloc[0] >= mydf['macd_9ema_signal'].iloc[0]:
            listLongShort.append("SELL")
            print("MACD Call for ",nseid, " - ", listLongShort )                
            returnDf['macd_prediction'] = ['SELL']
        else:
            listLongShort.append("HOLD")
            returnDf['macd_prediction'] = ['HOLD']
            
            
        return returnDf             

    def potentilaBuySellCall(self, df,rDf):
        
        mydf = df[-2:] # take last 2 records
        rsiDf = rDf[-5:] # take last 5 records
        
        nseid = mydf['nseid'].iloc[1]
        prev_close = mydf['close'].iloc[1]
        returnDf = pd.DataFrame()
        returnDf['nseid'] = [nseid]
        returnDf['prev_close'] = [prev_close]
        returnDf['my_date'] = [pd.to_datetime('today')]
        
#        print (mydf)
        listLongShort = []    # Since you need at least two days in the for loop
        
        ###############  MACD Logic ############################################
        
        macd_0 = mydf['MACD'].iloc[0]
        macd_sig_0 = mydf['macd_9ema_signal'].iloc[0]        
        macd_1 = mydf['MACD'].iloc[1]
        macd_sig_1 = mydf['macd_9ema_signal'].iloc[1]
        pos_threshold = 0.3
        neg_threshold = -0.3
        
        diff_1 = macd_1 - macd_sig_1
        diff_0 = mydf['MACD'].iloc[0] - mydf['macd_9ema_signal'].iloc[0]

        if macd_1 < 0 and diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :
#        if diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :            
            listLongShort.append("Potential BUY")
            print("MACD Call for ",nseid, " - ", listLongShort )   
#            print("macd_1 > ", macd_1, ", macd_sig_1 > ",macd_sig_1,",  diff_1 > ",diff_1, ", diff_0 > ", diff_0)             
#            print (mydf)
            returnDf['macd_prediction'] = ['Potential Buy']
            
        elif macd_1 > 0 and diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :
#        elif diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :    
            listLongShort.append("Potential SELL")
            print("MACD Call for ",nseid, " - ", listLongShort )          
#            print("macd_1 > ", macd_1, ", macd_sig_1 > ",macd_sig_1,",  diff_1 > ",diff_1, ", diff_0 > ", diff_0)             
#            print (mydf)
            returnDf['macd_prediction'] = ['Potential Sell']
#        else:
#            listLongShort.append("HOLD")
#            returnDf['macd_prediction'] = ['HOLD']
            
        ##  Strong BUY or SELL    
        if macd_1 > macd_sig_1 and macd_0 <= macd_sig_0:
            listLongShort.append("BUY")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['macd_prediction'] = ['BUY']             
        elif macd_1 < macd_sig_1 and macd_0 >= macd_sig_0:
            listLongShort.append("SELL")
            print("MACD Call for ",nseid, " - ", listLongShort )                
            returnDf['macd_prediction'] = ['SELL']
#        else:
#            listLongShort.append("HOLD")
#            returnDf['macd_prediction'] = ['HOLD']
#            
            
        
        ######## RSI logic ################################################
        print(rsiDf)
        rs0 = rsiDf['rsi'].iloc[0]
        rs1 = rsiDf['rsi'].iloc[1]
        rs2 = rsiDf['rsi'].iloc[2]
        rs3 = rsiDf['rsi'].iloc[3] # second last
        rs4 = rsiDf['rsi'].iloc[4] # this is last
        
        if rs4 < 70 and rs4 > 50 and rs4 < rs3 and rs3 < rs2  :
            print("RSI Call for ",nseid, " - SELL")          
            returnDf['rsi_prediction'] = ['SELL']
        elif rs4 > 70 and rs4 < rs3 and rs3 < rs2  :
            print("RSI Call for ",nseid, " - Potential Sell")          
            returnDf['rsi_prediction'] = ['Potential Sell']
            
            
        
        
        return returnDf        
        
#    def volume_average(self, df):
#        df2 = pd.DataFrame()    
#        df2['15dma'] = df['close'].rolling(window=50).mean()
#        df2['20dma'] = df['close'].rolling(window=20).mean()
#        return df2    

    def MACD(self, df):
        df2 = pd.DataFrame()
        temp1 = pd.DataFrame()
        temp2 = pd.DataFrame()

        
#        temp1['5dma_vol'] = df['volume'].rolling(window=5).mean().dropna()
#        
#        temp2['10dma_vol'] = df['volume'].rolling(window=10).mean().dropna()
#        
#        
#        temp1 = temp1.reset_index(drop=True)
#        temp2 = temp2.reset_index(drop=True)
#        df2 = temp1.join(temp2)
        df2['Date'] = df['my_date']
        df2['close'] = df['close']
        df2['26ema'] = df['close'].ewm(span=26, min_periods=1).mean()
        df2['12ema'] = df['close'].ewm(span=12, min_periods=1).mean()

            
#        df2['26ema'] = pd.ewma(df['close'], span=26)
#        df2['12ema'] = pd.ewma(df['close'], span=12)

        
        df2['MACD'] = df2['12ema'] - df2['26ema']
        df2 = df2.drop(['26ema', '12ema'],1 )
#        print ("df2 - ", df2)
        #Now calculate 9 ema signal line
        df2['macd_9ema_signal'] = df2['MACD'].ewm(span=9, min_periods=1).mean()
        df2['macd_signal_diff'] = df2['MACD'] - df2['macd_9ema_signal']
        
        return df2          
        
    def saveInDB(self, df):
        newDf = pd.DataFrame()
        for i, row in df.iterrows():
            nseid = row.nseid
            #only get live data for NON hold predictions
            macd_pred = row.macd_prediction

            if not macd_pred == 'HOLD':
                liveData = self.module_Get_Live_Data_From_Google.getLiveQuotesForAStock(nseid)
                change = liveData.get('c')
#                print ("change - ",change)
                if change is not None :
                    close = float(liveData.get('l'))
                    row['close'] = close
                    change = float(liveData.get('c'))
        #            current_price = liveData.get('l')
                    if change > 0:
#                        df.set_value(i,'actual','Buy')
                        row['actual'] = 'Buy'
                    elif change < 0:
#                        df.set_value(i,'actual','Sell')
                        row['actual'] = 'Sell'
                    else :
#                        df.set_value(i,'actual','Neutral')
                        row['actual'] = 'Neutral'
                    
                    newDf = newDf.append(row)    
                    
                    
        #now save in DB   
        print ('NewDF to be saved- \n', newDf)             
        newDf.to_sql('stock_macd_rsi_analysis', self.engine, if_exists='append', index=False) 

        


    def calculateRSI(self, df):
        n=14
        delta = df['close'].diff()
        dUp, dDown = delta.copy(), delta.copy()
        dUp[dUp < 0] = 0
        dDown[dDown > 0] = 0
        
        RolUp = pd.rolling_mean(dUp, n)
        RolDown = pd.rolling_mean(dDown, n).abs()
        
        RS = RolUp / RolDown
        RSI = 100.0 - (100.0 / (1.0 + RS))
#        print("RSI - ", RSI)
        df['rsi'] = RSI
        return df
        
            

       
        
thisObj = Process_MACD_Volume_Analysis()
stock_names= thisObj.getStockList()
#print ("stock_names - \n", stock_names)

outputDf = thisObj.calculateMACDAndRSI(stock_names)

#print ("\n\n\n Final List- \n", outputDf)
thisObj.saveInDB(outputDf )
print ("Saved results in DB" )

    