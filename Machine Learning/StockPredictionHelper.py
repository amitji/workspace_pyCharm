
import quandl
import pandas as pd
import nsepy as nse
from datetime import date, timedelta

class StockPredictionHelper:
    
#    def __init__(self):
#        self.result_df = [] 
        
        
    
    def getData(self, ticker, flag):
        
        if flag == 'real_time':
            
            Authkey = '5_pRK9pKefuvZzHe-MkSy'
            
            nse_dataset = "NSE" + "/" + ticker
    #        mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=1000, sort_order="asc")
            mydata = quandl.get(nse_dataset, authtoken=Authkey, start_date='2014-01-01', sort_order="asc")
            
            
            # print mydata
            df = pd.DataFrame(mydata)
            print (ticker, " first record - \n", df[:1])
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
    #        df.columns = ['open', 'high','low','close','volume','turnover']
            df.columns = ['date','open', 'high','low','close','volume','turnover']
            print ('Got data from Quandl')
            
        else:
            df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NIFTY_50.csv")
            df.columns = ['date','open', 'high','low','close','volume','turnover']
            df = df.drop(['date'],1)
            print ('Got data from CSV')
        
    #    print(df.columns)
        #'Open', 'High', 'Low', 'Close', 'Shares Traded', 'Turnover (Rs. Cr)'
        
        #dd = df.to_dict(orient='dict')
        #ddd = dd['Close'] 
        
    #    print (df)
        return df
    
    
    def getNiftyData(self,ticker, flag):
        
        if flag == 'real_time':
            
            Authkey = '5_pRK9pKefuvZzHe-MkSy'
            
            nse_dataset = "NSE" + "/" + ticker
            mydata = quandl.get(nse_dataset, authtoken=Authkey, start_date='2014-01-01', sort_order="asc")
            
            # print mydata
            df = pd.DataFrame(mydata)
            print ("Nifty first record - \n", df[:1])
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
            df.columns = ['open', 'high','low','close','volume','turnover']
    #        df.columns = ['date','open', 'high','low','close','volume','turnover']
            print ('Got data from Quandl')
            
        else:
            df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NIFTY_50.csv")
            df.columns = ['date','open', 'high','low','close','volume','turnover']
            df = df.drop(['date'],1)
            print ('Got data from CSV')
         
        return df   
        
    
    def getDataFromNSE(self,ticker, flag):
        
            mydata = nse.get_history(symbol=ticker, 
                        start=date(2017,1,1), 
                        end=date(2017,12,31),
    					index=True)
            df = pd.DataFrame(mydata)
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
    #        df.columns = ['open', 'high','low','close','volume','turnover']
            df.columns = ['open', 'high','low','close','volume','turnover']
            print ('Got data from Quandl')
    

    def moving_average(self, df):
        #MSE does not change using 4 below
#        df['1dma'] = df['close'].rolling(window=1).mean()
#        df['2dma'] = df['close'].rolling(window=2).mean()
#        df['3dma'] = df['close'].rolling(window=3).mean()
#        df['4dma'] = df['close'].rolling(window=4).mean()
        df['5dma'] = df['close'].rolling(window=5).mean()
        df['10dma'] = df['close'].rolling(window=10).mean()
        df['15dma'] = df['close'].rolling(window=15).mean()
        df['20dma'] = df['close'].rolling(window=20).mean()
        df['50dma'] = df['close'].rolling(window=50).mean()
        #following two seems to be important as MSE drop using them
        df['100dma'] = df['close'].rolling(window=100).mean()
        df['200dma'] = df['close'].rolling(window=200).mean()

        return df    

    def MACD(self, df):
        
#        df['26ema'] = pd.ewma(df["close"], span=26)
        df['26ema'] = df['close'].ewm(span=26).mean()
#        df['12ema'] = pd.ewma(df["close"], span=12)
        df['12ema'] = df['close'].ewm(span=12).mean()
        
        df['MACD'] = df['12ema'] - df['26ema']
        
        #Now calculate 9 ema signal line
        df['macd_9ema_signal'] = df['MACD'].ewm(span=9).mean()
        df['macd_signal_diff'] = df['macd_9ema_signal']  - df['MACD']
#        df = df.drop(['26ema', '12ema'],1 )
        df = df.drop(['26ema', '12ema', 'MACD','macd_9ema_signal'],1 )
        
        
#        df.plot(y = ["MACD"], title = "MACD")
        return df  
    
    def predictNextDayPrice(self):
        
        df = self.getData('NIFTY_50')
        
    def addChangeFromPreviousDay(self, df):
        
        df['prev_day_close'] = df['close'].shift(1)
        df['change'] = df['close'] - df['prev_day_close']
        df = df.drop(['prev_day_close'],1 )
        return df
    
    def addNiftyCloseAsFeature(self, df, flag):
        temp = self.getNiftyData("NIFTY_50", flag)
        #Drop all column but 'close'
        temp = temp[['close']]
        temp=temp.rename(columns = {'close':'nifty_close'})
        #concate does an outerjoin.. this is case where some records in Nist proces missing
        # for few dates
        df = pd.concat([df, temp], axis=1)
#        df['nifty_close'] = temp['close']
        return df