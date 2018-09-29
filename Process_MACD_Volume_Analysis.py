# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 06:28:00 2018

@author: amahe6
"""
import pandas as pd
import DBManager
import Module_Get_Live_Data_From_Google
import time
import EmailUtil

class Process_MACD_Volume_Analysis:
    
    def __init__(self):
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()
        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()

    def getStockList(self):

        ##Get all stocks from amit_portfolio and all FO stocks         
        #prod sql
        select_sql = "select symbol nseid, 'n' owned from stocksdb.fo_mktlots fo where  not exists (select nseid from stocksdb.amit_portfolio ap where ap.nseid = fo.symbol ) "
        select_sql += " union select nseid, 'y' owned from stocksdb.amit_portfolio sn where sn.is_inactive='n' order by nseid  "
        
        #testing
#        select_sql = "select symbol nseid, 'y' owned from stocksdb.fo_mktlots sn where symbol in ('NCC')"
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
        
        
    def calculateMACDAndRSI(self,stock_names):
        outputDf = pd.DataFrame()
        for index, row in stock_names.iterrows():
            nseid = row.nseid
            df = self.getMarketDataForAStock(row['nseid'])
            
            if df.empty:
#                print("No Data for ", nseid)
                continue
            
            df = df.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
            rsiDf =   self.calculateRSI(df)
            
            newdf = self.MACD(df)
            newdf['nseid'] = nseid
            newdf['owned'] = row.owned
#            returnDf = self.potentilaBuySellCall(newdf, rsiDf)
            returnDf = self.potentilaBuySellCall_NEW(newdf, rsiDf)
            outputDf = outputDf.append(returnDf)
            
         
        return outputDf    

    def potentilaBuySellCall_NEW(self, df,rDf):
        
        mydf = df[-2:] # take last 2 records
        rsiDf = rDf[-5:] # take last 5 records
        
        nseid = mydf['nseid'].iloc[1]
        
        returnDf = pd.DataFrame()
        returnDf['nseid'] = [nseid]
        returnDf['owned'] = mydf['owned'].iloc[1]
        returnDf['volume'] = mydf['volume'].iloc[1]
        returnDf['5dma_vol'] = mydf['5dma_vol'].iloc[1]
        returnDf['10dma_vol'] = mydf['10dma_vol'].iloc[1]
        
        returnDf['close'] = mydf['close'].iloc[1]
        returnDf['prev_close'] = mydf['close'].iloc[0]
        returnDf['my_date'] = [pd.to_datetime('now')+pd.Timedelta('05:30:00')]
        
        listLongShort = []    # Since you need at least two days in the for loop
        
        ###############  MACD Logic ############################################
        
        macd_0 = mydf['MACD'].iloc[0] # second last record
        macd_sig_0 = mydf['macd_9ema_signal'].iloc[0]        
        macd_1 = mydf['MACD'].iloc[1] # last record
        macd_sig_1 = mydf['macd_9ema_signal'].iloc[1] 
        pos_threshold = 0.8# 0.3  # 
        neg_threshold = -0.8 # -0.3  #-1.5627, -2
        
        diff_1 = macd_1 - macd_sig_1   
        diff_0 = macd_0 - macd_sig_0  

        returnDf['macd_prediction'] = ""
#        if macd_1 < 0 and diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :
        if diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :            
            listLongShort.append("Potential Buy")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['macd_prediction'] = ['Potential Buy']
            
        elif macd_1 > 0 and diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :
#        elif diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :    
            listLongShort.append("Potential Sell")
            print("MACD Call for ",nseid, " - ", listLongShort )          
            returnDf['macd_prediction'] = ['Potential Sell']
            
        ##  Strong Buy or Sell    
        if macd_1 > macd_sig_1 and macd_0 <= macd_sig_0:
            listLongShort.append("Buy")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['macd_prediction'] = ['Buy']             
        elif macd_1 < macd_sig_1 and macd_0 >= macd_sig_0:
            listLongShort.append("Sell")
            print("MACD Call for ",nseid, " - ", listLongShort )                
            returnDf['macd_prediction'] = ['Sell']
#        else:
#            listLongShort.append("HOLD")
#            returnDf['macd_prediction'] = ['HOLD']
            
        
        ######## RSI logic ################################################
#        print(rsiDf)
#        rsi0 = rsiDf['rsi'].iloc[0]
#        rsi1 = rsiDf['rsi'].iloc[1]
        rsi2 = rsiDf['rsi'].iloc[2]
        rsi3 = rsiDf['rsi'].iloc[3] # second last
        rsi4 = rsiDf['rsi'].iloc[4] # this is last
        
        returnDf['rsi_prediction'] = ""
#        if rsi4 < 70 and rsi4 > 50 and rsi4 < rsi3 and rsi3 < rsi2 and rsi2 > 70  :
        if  rsi4 > 50 and rsi4 < rsi3 and rsi3 < rsi2 and rsi2 > 70  :    
            print("RSI Call for ",nseid, " - Sell")          
            returnDf['rsi_prediction'] = ['Sell']
#        elif rsi4 > 70 and rsi4 < rsi3 and rsi3 < rsi2  :
        elif rsi4 > 70 and rsi4 < rsi3 :
            print("RSI Call for ",nseid, " - Potential Sell")          
            returnDf['rsi_prediction'] = ['Potential Sell']
#        elif rsi4 > 30 and rsi4 < 50 and rsi3 < rsi4 and  rsi2 < rsi3 and rsi2 < 30  :
        elif rsi4 < 50 and rsi3 < rsi4 and  rsi2 < rsi3 and rsi2 < 30  :    
            print("RSI Call for ",nseid, " - Buy")          
            returnDf['rsi_prediction'] = ['Buy']
#        elif rsi4 < 30 and rsi4 > 20 and rsi3 < rsi4  :
        elif rsi4 < 30 and rsi3 < rsi4  :    
            print("RSI Call for ",nseid, " - Potential Buy")          
            returnDf['rsi_prediction'] = ['Potential Buy']
            
            
        
        # add all parameters
        returnDf['macd_1'] = macd_1
        returnDf['macd_0'] = macd_0
        returnDf['macd_sig_1'] = macd_sig_1
        returnDf['macd_sig_0'] = macd_sig_0
        returnDf['diff_1'] = diff_1
        returnDf['diff_0'] = diff_0
        returnDf['rsi4'] = rsi4
        returnDf['rsi3'] = rsi3
        returnDf['rsi2'] = rsi2        
        
        return returnDf        

    def potentilaBuySellCall(self, df,rDf):
        
        mydf = df[-2:] # take last 2 records
        rsiDf = rDf[-5:] # take last 5 records
        
        nseid = mydf['nseid'].iloc[1]
        
        returnDf = pd.DataFrame()
        returnDf['nseid'] = [nseid]
        returnDf['owned'] = mydf['owned'].iloc[1]
        returnDf['volume'] = mydf['volume'].iloc[1]
        returnDf['5dma_vol'] = mydf['5dma_vol'].iloc[1]
        returnDf['10dma_vol'] = mydf['10dma_vol'].iloc[1]
        
        returnDf['close'] = mydf['close'].iloc[1]
        returnDf['prev_close'] = mydf['close'].iloc[0]
        returnDf['my_date'] = [pd.to_datetime('now')+pd.Timedelta('05:30:00')]
        
        listLongShort = []    # Since you need at least two days in the for loop
        
        ###############  MACD Logic ############################################
        
        macd_0 = mydf['MACD'].iloc[0] # second last record
        macd_sig_0 = mydf['macd_9ema_signal'].iloc[0]        
        macd_1 = mydf['MACD'].iloc[1] # last record
        macd_sig_1 = mydf['macd_9ema_signal'].iloc[1] 
        pos_threshold = 0.3
        neg_threshold = -0.3
        
        diff_1 = macd_1 - macd_sig_1
        diff_0 = macd_0 - macd_sig_0

        returnDf['macd_prediction'] = ""
        if macd_1 < 0 and diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :
#        if diff_1 < 0 and diff_0 < diff_1 and  (neg_threshold < diff_1  < 0) :            
            listLongShort.append("Potential Buy")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['macd_prediction'] = ['Potential Buy']
            
        elif macd_1 > 0 and diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :
#        elif diff_1 > 0 and diff_0 > diff_1 and (0 < diff_1  < pos_threshold) :    
            listLongShort.append("Potential Sell")
            print("MACD Call for ",nseid, " - ", listLongShort )          
            returnDf['macd_prediction'] = ['Potential Sell']
            
        ##  Strong Buy or Sell    
        if macd_1 > macd_sig_1 and macd_0 <= macd_sig_0:
            listLongShort.append("Buy")
            print("MACD Call for ",nseid, " - ", listLongShort )   
            returnDf['macd_prediction'] = ['Buy']             
        elif macd_1 < macd_sig_1 and macd_0 >= macd_sig_0:
            listLongShort.append("Sell")
            print("MACD Call for ",nseid, " - ", listLongShort )                
            returnDf['macd_prediction'] = ['Sell']
#        else:
#            listLongShort.append("HOLD")
#            returnDf['macd_prediction'] = ['HOLD']
            
        
        ######## RSI logic ################################################
#        print(rsiDf)
#        rsi0 = rsiDf['rsi'].iloc[0]
#        rsi1 = rsiDf['rsi'].iloc[1]
        rsi2 = rsiDf['rsi'].iloc[2]
        rsi3 = rsiDf['rsi'].iloc[3] # second last
        rsi4 = rsiDf['rsi'].iloc[4] # this is last
        
        returnDf['rsi_prediction'] = ""
        if rsi4 < 70 and rsi4 > 50 and rsi4 < rsi3 and rsi3 < rsi2 and rsi2 > 70  :
            print("RSI Call for ",nseid, " - Sell")          
            returnDf['rsi_prediction'] = ['Sell']
        elif rsi4 > 70 and rsi4 < rsi3 and rsi3 < rsi2  :
            print("RSI Call for ",nseid, " - Potential Sell")          
            returnDf['rsi_prediction'] = ['Potential Sell']
        elif rsi4 > 30 and rsi4 < 50 and rsi3 < rsi4 and  rsi2 < rsi3 and rsi2 < 30  :
            print("RSI Call for ",nseid, " - Buy")          
            returnDf['rsi_prediction'] = ['Buy']
        elif rsi4 < 30 and rsi4 > 20 and rsi3 < rsi4  :
            print("RSI Call for ",nseid, " - Potential Buy")          
            returnDf['rsi_prediction'] = ['Potential Buy']
            
            
        
        # add all parameters
        returnDf['macd_1'] = macd_1
        returnDf['macd_0'] = macd_0
        returnDf['macd_sig_1'] = macd_sig_1
        returnDf['macd_sig_0'] = macd_sig_0
        returnDf['rsi4'] = rsi4
        returnDf['rsi3'] = rsi3
        returnDf['rsi2'] = rsi2
        
        
        
        return returnDf        
          

    def MACD(self, df):
        df2 = pd.DataFrame()
        temp1 = pd.DataFrame()
        temp2 = pd.DataFrame()

        
        temp1['5dma_vol'] = df['volume'].rolling(window=5).mean()
        temp2['10dma_vol'] = df['volume'].rolling(window=10).mean()
        temp1 = temp1.reset_index(drop=True)
        temp2 = temp2.reset_index(drop=True)
        
        df2 = temp1.join(temp2)
        df2['volume'] = df['volume']
        df2['Date'] = df['my_date']
        df2['close'] = df['close']
        df2['26ema'] = df['close'].ewm(span=26, min_periods=1).mean()
        df2['12ema'] = df['close'].ewm(span=12, min_periods=1).mean()

            
        df2['MACD'] = df2['12ema'] - df2['26ema']
        df2 = df2.drop(['26ema', '12ema'],1 )
        #Now calculate 9 ema signal line
        df2['macd_9ema_signal'] = df2['MACD'].ewm(span=9, min_periods=1).mean()
        df2['macd_signal_diff'] = df2['MACD'] - df2['macd_9ema_signal']
        
        return df2          

    
    def saveInDB_LiveData(self, df):
        newDf = pd.DataFrame()
        for i, row in df.iterrows():
            nseid = row.nseid
#            fullid = "NSE:"+nseid
            #only get live data for NON hold predictions
            macd_pred = row.macd_prediction

            if not macd_pred == 'HOLD':
                print('Getting quotes from Quandl...................\n')
#                liveData = self.module_Get_Live_Data_From_Google.getLiveQuotesForAStock(nseid)
                liveData = self.module_Get_Live_Data_From_Google.getQuoteFromQuandl(nseid) 
#                liveData = liveDict.items()
                print('liveData - ',liveData )
                change = liveData.get('c')
                if change is not None :
                    close = float(liveData.get('l'))
                    row['close'] = close
                    row['prev_close'] = float(liveData.get('pcls'))
                    change = float(liveData.get('c'))
                    if change > 0:
                        row['actual'] = 'Buy'
                    elif change < 0:
                        row['actual'] = 'Sell'
                    else :
                        row['actual'] = 'Neutral'
                    
                    #Calculate Outcome 
                    """                
                    row['outcome']=''
                    if row.macd_prediction == row.actual and row.rsi_prediction == row.actual:
                         row['outcome'] = 1
                    #treat Potetial Sel  and sell same     
                    elif row.macd_prediction.find(row.actual) != -1 and row.rsi_prediction.find(row.actual) != -1:
                         row['outcome'] = 1
                    elif row.macd_prediction == "" and row.rsi_prediction == "":  # both empty
                         row['outcome'] = 4
                    elif row.macd_prediction.find(row.actual) != -1 or row.rsi_prediction.find(row.actual) != -1:
                         row['outcome'] = 2
                    else:
                         row['outcome'] = 3
                    """
                    #New way of Calculating outcome
                    row['outcome']=''
                    print('row.macd_prediction - ',row.macd_prediction )
                    print('row.rsi_prediction - ',row.rsi_prediction )
                    
                    if row.macd_prediction == "" and row.rsi_prediction == "":  # both empty
                         row['outcome'] = 0
                    elif row.macd_prediction == row.rsi_prediction :  #if RSI and MACD telling same thing then most preferred.
                         row['outcome'] = 1
                    #treat Potetial Sel  and sell same     
                    elif row.macd_prediction.find(row.rsi_prediction) != -1 and (row.macd_prediction != "" and row.rsi_prediction != ""):
                         row['outcome'] = 1                         
                    elif row.macd_prediction != "" or row.rsi_prediction != "":
                         row['outcome'] = 2
                    else:
                         row['outcome'] = 0
                         
                         
                         
                    newDf = newDf.append(row)    
                    
                    
        #now save in DB   
#        print ('NewDF to be saved- \n', newDf)
        filename = 'output\\MACD_Volume_Analysis.csv'
        newDf.to_sql('stock_macd_rsi_analysis', self.engine, if_exists='append', index=False)         
        newDf.to_csv(filename)

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
start_time = time.time()
stock_names= thisObj.getStockList()
#print ("stock_names - \n", stock_names)

outputDf = thisObj.calculateMACDAndRSI(stock_names)

#print ("\n\n\n Final List- \n", outputDf)
thisObj.saveInDB_LiveData(outputDf )

print ("\n\n****** Saved results in DB *****************" )
print("Total time taken by process --- %s seconds ---" % (time.time() - start_time))
EmailUtil.send_email_as_text("Process_MACD_Volume_Analysis.py","" , "http://localhost/stockcircuitserver/php/report_macd_rsi_analysis.php")
    