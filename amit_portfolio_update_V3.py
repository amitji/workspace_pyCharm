
#import urllib2
import DBManager
import pandas as pd
import numpy as np
from datetime import datetime, time
import EmailUtil
import time as t
import Module_Get_Live_Data_From_Google
import Module_Get_Live_Data_From_Zerodha
from kiteconnect import KiteConnect
import zerodha_const as zc
import sys
import ModuleAmitException
import winsound


class GoogleFinanceAPI:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.engine = DBManager.createEngine()
        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()
        self.highVolumeStocks = pd.DataFrame()
        self.volumeForImpStocks = pd.DataFrame()
        pd.set_option('expand_frame_repr', False)
        
    def getStockList(self):
        
#        Prod sql
        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where is_index='n' and is_inactive != 'y' order by display_seq "
        
        #testing
#        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where nseid in ('CAPF','JSWSTEEL','ITC', 'HCC') "
#        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where is_fo='y' "
#        select_sql ="select fullid, nseid from stocksdb.stock_names where nseid in ('RBLBANK','HAVELLS','ITC') "
        
        self.cur.execute(select_sql)
        rows = self.cur.fetchall()
        data = [] # list
        for row in rows:
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            data.append(dd)
        return data

    def getFOStockList(self):
        print ("\n\n***************  Running it for only FO stocks\n\n")
        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where display_seq is not null and is_fo = 'y' and is_inactive != 'y'  order by display_seq "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            data.append(dd)
        # print data
        return data

    '''
    def get(self, symbol):
        url = self.prefix + "%s" % ( symbol)
        u = urllib2.urlopen(url)
        content = u.read()

        obj = json.loads(content[3:])
        return obj[0]
    '''
    def getMarketDataForAStock(self, nseid):
        
        select_sql = "select * from stocksdb.stock_market_data smd where smd.nseid = '%s' order by my_date  " % (nseid)
        df = pd.read_sql(select_sql, self.con)
        return df  

    def getMarketDataForStocks(self, stock_names):
        
        #Get prev day Vol, 5 days avg vol for each stock
        outputDf = pd.DataFrame()
        self.highVolumeStocks = pd.DataFrame()
        for row in stock_names:
#            fullid = row['fullid']
#            nseid = row['nseid']
            df = self.getMarketDataForAStock(row['nseid'])
            
            if df.empty:
                continue
            
            fulldf = df.drop(['id','high', 'low', 'open','last', 'turnover', 'last_modified'], axis=1)
            fulldf['5d_avg_vol'] = fulldf['volume'].rolling(window=5).mean()
            newdf = fulldf[-1:]
            outputDf = outputDf.append(newdf)
        
        print(outputDf)
        return  outputDf 
    
    def doIntraDayVolumeAnalysis_New(self, market_data,allQuotes, imp_stocks_to_watch):
        
        
        outputDf = pd.DataFrame()
        self.highVolumeStocks = pd.DataFrame()
        self.volumeForImpStocks = pd.DataFrame()
        
        allQuotesDf = pd.DataFrame(allQuotes)
        for index, row in  market_data.iterrows():
#            fullid = row['fullid']
            try:
                nseid = row['nseid']
                
                quotedf = allQuotesDf.loc[allQuotesDf['nseid'] == nseid]
                curr_vol =  float(quotedf['volume'].values[0])
                
                prev_vol = float(row['volume'])
                fiveDay_avg_vol = float(row['5d_avg_vol'])
                
    #            print(curr_vol, prev_vol, fiveDay_avg_vol )
                pc_from_prev = round(((curr_vol - prev_vol)/prev_vol)*100 , 2)
                pc_from_5day_avg_vol = round(((curr_vol - fiveDay_avg_vol)/fiveDay_avg_vol)*100,2)
                outputDf['nseid'] = [nseid]
                outputDf['curr_vol'] = [curr_vol]
                outputDf['prev_vol'] = [prev_vol]
                outputDf['fiveDay_avg_vol'] = [fiveDay_avg_vol]
                outputDf['pc_from_prev'] = [pc_from_prev]
                outputDf['pc_from_5day_avg_vol'] = [pc_from_5day_avg_vol]
                outputDf['last_trade'] = [quotedf['l'].values[0]]
                outputDf['change'] = [quotedf['c'].values[0]]
                outputDf['chg_perct'] = [quotedf['cp'].values[0]]
                
                
                if(curr_vol > prev_vol or curr_vol > fiveDay_avg_vol):
#                    print("\nCurrent Vol is high for ", nseid) 
#                    print(outputDf)
                    self.highVolumeStocks = self.highVolumeStocks.append(outputDf)
    #            else:
    #                print("Current Vol is less than prev or 5 day Avg for ", nseid)
                
                elif nseid in imp_stocks_to_watch:
                    self.volumeForImpStocks = self.volumeForImpStocks.append(outputDf)
                    
            except Exception as e1:
                print ("\n******Exception in doIntraDayVolumeAnalysis_New for - ", nseid, curr_vol )
                print (str(e1))
                ModuleAmitException.printInfo()
                pass

        self.highVolumeStocks.to_sql('intraday_volume_details', self.engine, if_exists='append', index=False)
        print("highVolumeStocks saved in DB")  
        
        print("\n****** Volume Info for Important Stocks - \n", self.volumeForImpStocks)
        return  self.highVolumeStocks 
              

    def saveIntoDB(self, allQuotes):

        print ("\n*** Amit saving qoutes to database")
        records = []
        for row in allQuotes:
            try:
                record = (( row['l'], row['c'],row['cp'],row['pcls'],row['volume'], row['fullid']))
                records.append(record )
            except Exception as e:
                print ("\n******Amit saveIntoDB, some issue with quotes, row data - ", row)
                print (str(e))
                pass
        #use batch execute rahter above  1by 1
        sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s, volume=%s where fullid=%s"
        self.cur.executemany(sql, records)
        self.con.commit()
        print("\n\n**** Save in DB successful")

    def in_between(self,now, start, end):
        if start <= end:
            return start <= now < end
        else:  # over midnight e.g., 23:30-04:15
            return start <= now or now < end

    def printZerodhaAccess_token(self, request_token):
        kite = KiteConnect(api_key="l5r6aemba2mjr14s")
#        print(kite.login_url())
        data = kite.generate_session(request_token, zc.secret_key)
        print(data)
        sys.exit()

if __name__ == "__main__":
    c = GoogleFinanceAPI()

    stock_names = c.getStockList()
    #Get only FO when Google is slow...
#    stock_names = c.getFOStockList()

    records = [] ## LIST OF LISTS
    minutes_count = 0  # compare with 7 Hrs run daily from 9-4 pm (7*60=420)
    #EmailUtil.send_email_as_text(" amit_portfolio_update.py job started - ", "", "")
    print ("\n*** Processing ", len(stock_names), " Stocks" )
    
    getDataFromQuandl = 0
    
    getDataFromZerodha = 1

    if getDataFromZerodha == 1:
        marketData = c.getMarketDataForStocks(stock_names)
        module_Get_Live_Data_From_Zerodha = Module_Get_Live_Data_From_Zerodha.Module_Get_Live_Data_From_Zerodha()
         
        #Amit   
#        c.printZerodhaAccess_token('eqOYRTR02WyJQIj2EI01neXmwef4YwDs')
        
        while (c.in_between(datetime.now().time(), time(4,30), time(18,40))):
            print ("\n*** Getting quotes from ZERODHA one by one ************")
            start_time = t.time()
            allQuotes = module_Get_Live_Data_From_Zerodha.getAllQuotesFromZerodha(stock_names)
#            print(allQuotes)
     
            if allQuotes:
                try:
                    c.saveIntoDB(allQuotes)
                    #Imp stocks to be watched for Vol Movements
                    imp_stocks_to_watch = ['BERGEPAINT','UBL','VOLTAS','NCC','ITC','BATAINDIA', 'JINDALSTEL', 'GMRINFRA','KAJARIACER','RBLBANK',
                               'LUPIN','CADILAHC','DCBBANK','SREINFRA','YESBANK','HEXAWARE', 'KPIT', 'RELINFRA', 'GRANULES']

                    df = c.doIntraDayVolumeAnalysis_New(marketData,allQuotes, imp_stocks_to_watch) 
                    print("\n\n ******************* Stocks with High Volume - \n", df)
                    
                    frequency = 500  # Set Frequency To 2500 Hertz
                    duration = 500  # Set Duration To 1000 ms == 1 second
                    winsound.Beep(frequency, duration)
                                                       
                except Exception as e1:
                    print ("\n******Exception in saving quotes in DB, sleep for a minute and try...\n\n\n" )
                    print (str(e1))
                    ModuleAmitException.printInfo()
                    #May be Db conn has gone bad so close it and initialize again.. Lets try !
                    c.con.close()                    
                    c.con = DBManager.connectDB()
                    c.cur = c.con.cursor()

                    t.sleep(60)
                    try:
                        c.saveIntoDB(allQuotes)
                    except Exception as e2: 
                        print ("\n******Second time Exception in saving quotes in DB, skip this cycle and continue...\n\n\n" )    
                        print (str(e1))
                        ModuleAmitException.printInfo()
                        EmailUtil.send_email_as_text(" Save ib DB is failing amit_portfolio_update.py, Check Now", "", "")
  
            else:
                print ("\n*** Amit All Quotes from Zerodha were empy(due to exception i guess) so not saving in DB")

            if minutes_count == 0:
                EmailUtil.send_email_as_text(" amit_portfolio_update.py job started - ", "", "")
            minutes_count = minutes_count+1
            print ("\n*** Amit Sleeping for 2 minute, remaining loops (420-x)- ", 420- minutes_count, " | Time - ", datetime.now())
            t.sleep(60)
       
#            print ("Done !!")
#            print("--- %s seconds ---" % (t.time() - start_time))
#            print("--- Minutes ---", float(t.time() - start_time)/60)
        
    elif getDataFromQuandl == 1:
#        stock_names = c.getStockList()
        print ("\n*** Getting quotes from Quandle ************")
        allQuotes = c.module_Get_Live_Data_From_Google.getAllQuotesFromQuandl(stock_names)
        if allQuotes:
            c.saveIntoDB(allQuotes)
#            print("Save in DB disabled")
        else:
            print ("\n*** Amit All Quotes from Google were empy(due to exception i guess) so not saving in DB")
    else:
        # get quandl quotes only once since we need previous days data whihc is not available from the 
        # url we use in following fn
#        quandlQuotes = c.module_Get_Live_Data_From_Google.getAllQuotesFromQuandl(stock_names)
        print ("\n*** Getting quotes from Quandle (One time) ************")
        mylist = c.module_Get_Live_Data_From_Google.getAllQuotesFromQuandl(stock_names)
        quandlData = pd.DataFrame(mylist)
        print('quandlData  - \n',quandlData )
        #while minutes_count < 420:
        #Run b/w morning 9 am to 4:00 pm IST
        while (c.in_between(datetime.now().time(), time(8,40), time(19,00))):
            print ("\n*** Getting quotes from Google one by one ************")
            allQuotes = c.module_Get_Live_Data_From_Google.getLiveQuotesForMultipleStock(stock_names,quandlData)
#            allQuotes = c.getAllQuotesFromQuandl(stock_names)
            
            if allQuotes:
                c.saveIntoDB(allQuotes)
            else:
                print ("\n*** Amit All Quotes from Google were empy(due to exception i guess) so not saving in DB")
            if minutes_count == 0:
                EmailUtil.send_email_as_text(" amit_portfolio_update.py job started - ", "", "")
            minutes_count = minutes_count+1
            print ("\n*** Amit Sleeping for 2 minute, remaining loops (420-x)- ", 420- minutes_count, " | Time - ", datetime.now())
            t.sleep(60)

    
    print ("\n*** Amit Exiting the quote process...SINCE TIME is  - ", datetime.now().time())