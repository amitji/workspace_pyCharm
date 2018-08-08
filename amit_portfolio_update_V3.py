
#import urllib2
from urllib.request import urlopen,Request
import urllib.request
import json
import DBManager
import pandas as pd
import numpy as np
from datetime import datetime, time
import EmailUtil
import time as t
import Module_Get_Live_Data_From_Google
import Module_Get_Live_Data_From_Zerodha

class GoogleFinanceAPI:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.module_Get_Live_Data_From_Google = Module_Get_Live_Data_From_Google.Module_Get_Live_Data_From_Google()
        
        
        
        #https://finance.google.com/finance?q=NSE:NCC
#        self.url_prefix = "https://finance.google.com/finance?q="


    def getStockList(self):
#        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where display_seq is not null and is_inactive != 'y' order by display_seq "
        
        #Prod sql
        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where is_index='n' and is_inactive != 'y' order by display_seq "
        
        #testing
#        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where nseid in ('ITC', 'NCC') "
#        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where is_fo='y' "
        
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = [] # list
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            data.append(dd)
        # print data
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



    def saveIntoDB(self, allQuotes):

        print ("\n*** Amit saving following qoutes to database")
        records = []
        fullid= ""
        for row in allQuotes:

            try:
                #print "fullid - ", fullid
                print(row)
                record = (( row['l'], row['c'],row['cp'],row['pcls'],row['volume'], row['fullid']))
                print (record)
                records.append(record )

            except Exception as e:
                print ("\n******Amit saveIntoDB, some issue with quotes, row data - ", row)
                print (str(e))
                pass

        # for record in records:
        #     print record
        #     print record[0], record[1]
        #     sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s where nseid=%s"
        #     self.cur.execute(sql, tuple(record))

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
        while (c.in_between(datetime.now().time(), time(8,40), time(19,00))):
            print ("\n*** Getting quotes from ZERODHA one by one ************")

            start_time = t.time()
            print ("\n*** Getting quotes from Zerodha ************")
            module_Get_Live_Data_From_Zerodha = Module_Get_Live_Data_From_Zerodha.Module_Get_Live_Data_From_Zerodha()
    #        kite = module_Get_Live_Data_From_Zerodha.getKiteObject()
            allQuotes = module_Get_Live_Data_From_Zerodha.getAllQuotesFromZerodha(stock_names)
            print(allQuotes)
     
            if allQuotes:
                c.saveIntoDB(allQuotes)
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
        while (c.in_between(datetime.now().time(), time(8,40), time(16,00))):
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