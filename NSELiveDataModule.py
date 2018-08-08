
import quandl
import pandas
import operator
import ModuleAmitException
import numpy as np
import pandas as pd

def __init__(self):
    print ("Calling GetNSELiveData constructor")



def getNSELiveData( nseid):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'

    nseid = nseid.replace("&", "")
    nseid = nseid.replace("-", "_")
    nse_dataset = "NSE" + "/" + nseid
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=252, sort_order="desc")
    return mydata

def getQuandlData( fullid, nseid):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    exceptionDir = {"NIFTY":"NIFTY_50"}
    nseid = nseid.replace("&", "")
    nseid = nseid.replace("-", "_")
    
    #first check if the nseid is in expetions
    if nseid in exceptionDir:
        nseid = exceptionDir[nseid]
        
    tokens = fullid.split(':')
    exchange = tokens[0]
    ticker = tokens[1]
    if exchange == "NSE" :
        dataset = "NSE" + "/" + nseid
    elif exchange == "BOM" :
        #BSE/BOM526652
        dataset = "BSE" + "/" + exchange+ticker
    else:
        dataset = "NSE" + "/" + nseid
    
    mydata = quandl.get(dataset, authtoken=Authkey, rows=252, sort_order="desc")
    return mydata


def getHighLowClose(mydata):
    # print mydata
    df = pandas.DataFrame(mydata)
    dd = df.to_dict(orient='dict')
    ddd = dd['Close']

    # print df
    # print dd
    # print ddd
    # print max(ddd, key=ddd.get)
    maxKey = max(ddd.items(), key=operator.itemgetter(1))[0]
    maxVal = ddd.get(maxKey)
    print ("High52 - ", maxVal)

    # print min(ddd, key=ddd.get)
    minKey = min(ddd.items(), key=operator.itemgetter(1))[0]
    minVal = ddd.get(minKey)
    print( "Low52 - ", minVal)

    last_price = df['Close'][0]

    print( "Last Price - ", last_price)

    nsedata = dict()
    nsedata["high52"] = maxVal
    nsedata["low52"] = minVal
    nsedata["last_price"] = last_price
    return nsedata
    
    
def getLastDayParams(mydata,fullid,nseid):
    df = mydata[:2]
    try:
    #    df.columns = ['open', 'high','low','last','close','volume','turnover']
        if "NIFTY" in nseid:
            df = df[['Close', 'Shares Traded']]
        elif "NSE:" in fullid:
            df = df[['Close', 'Total Trade Quantity']]  
        else:
            df = df[['Close', 'No. of Shares']]
         
        df.columns = ['close', 'volume']
    #    df = df.drop(['open', 'high','low','turnover'],1)
    #   This sript is run india time and Quandl last record by tnat time is 
    # already prevous trading day so no need to shift(-1)
    
    #Amit- now that google has stopped api, we need both today and prev day data
        df['prev_close'] = df['close'].shift(-1)
#        df['prev_close'] = df['close']
        #following line is commented bcoz volume as coming NaN for NIFTY and hence df was getting empties
#        df = df.dropna(axis=0, how='any')
        df['change'] = df['close'] - df['prev_close']
        df['percent_change'] = df['change'] / df['prev_close']
        df['percent_change'] = df['percent_change'].apply(lambda x: x*100)
    #    np.round(df, decimals=2)
        df = df.apply(lambda x: np.round(x, decimals=2))
        df['nseid'] = nseid
#    print (df)
    except Exception as e:
        print ("\n******Amit Exception in NSELiveDataModule::getLastDayParams  ")
        print (str(e))
        ModuleAmitException.printInfo()
        df.iloc[0:0]  # emptied the dataframe
        
    return df
    
    
    

    