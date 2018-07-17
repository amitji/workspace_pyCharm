# -*- coding: utf-8 -*-

import quandl
import pandas

def getData(ticker):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    
    nse_dataset = "NSE" + "/" + ticker
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=500, sort_order="asc")
    
    # print mydata
    df = pandas.DataFrame(mydata)
    
    print(df.columns)
    #'Open', 'High', 'Low', 'Close', 'Shares Traded', 'Turnover (Rs. Cr)'
    df.columns = ['open', 'high','low','close','volume','turnover']
    return df
