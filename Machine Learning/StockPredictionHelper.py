
import quandl
import pandas as pd
import nsepy as nse
from datetime import date, timedelta
import numpy as np
import glob

class StockPredictionHelper:
    
#    def __init__(self):
#        self.result_df = [] 
        
        
    
    def getData(self, ticker, real_time_flag):
        
        if real_time_flag == 1:
            
            Authkey = '5_pRK9pKefuvZzHe-MkSy'
            
            nse_dataset = "NSE" + "/" + ticker
    #        mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=1000, sort_order="asc")
            mydata = quandl.get(nse_dataset, authtoken=Authkey, start_date='2013-01-01', sort_order="asc")
            
            
            # print mydata
            df = pd.DataFrame(mydata)
#            print (ticker, " first record - \n", df[:1])
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
    #        df.columns = ['open', 'high','low','close','volume','turnover']
            df.columns = ['open', 'high','low','last','close','volume','turnover']
            print ('Got data from Quandl - ', ticker)
            
        else:
            df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NIFTY_50.csv")
            df.columns = ['date','open', 'high','low','close','volume','turnover']
            df = df.drop(['date'],1)
            print ('Got data from CSV - ', ticker)
        

        df = df.drop(['open','low','high','last', 'turnover','volume'],1)
        return df
 
    def getAnyDataFromQuandl(self, connection_string, ticker, start_date):
            
        Authkey = '5_pRK9pKefuvZzHe-MkSy'

#        mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=1000, sort_order="asc")
        mydata = quandl.get(connection_string, authtoken=Authkey, start_date=start_date, sort_order="asc")
        
        
        # print mydata
        df = pd.DataFrame(mydata)
#            print (ticker, " first record - \n", df[:1])
        filename = 'datasets\\'+ticker+'.csv'
        
        df.to_csv(filename)
#        df.columns = ['open', 'high','low','close','volume','turnover']
        df.columns = ['open', 'high','low','last','close','volume','turnover']
        print ('Got data from Quandl - ', ticker)

        df = df.drop(['open','low','high','last', 'turnover','volume'],1)
        return df
   
    
    def getNiftyData(self,ticker, real_time_flag):
        
        if real_time_flag == 1:
            
            Authkey = '5_pRK9pKefuvZzHe-MkSy'
            
            nse_dataset = "NSE" + "/" + ticker
            mydata = quandl.get(nse_dataset, authtoken=Authkey, start_date='2016-01-01', sort_order="asc")
            
            # print mydata
            df = pd.DataFrame(mydata)
#            print ("Nifty first record - \n", df[:1])
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
            df.columns = ['open', 'high','low','close','volume','turnover']
    #        df.columns = ['date','open', 'high','low','close','volume','turnover']
            print ('Got NIFTY ticker data from Quandl')
            
        else:
            df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NIFTY_50.csv")
            df.columns = ['date','open', 'high','low','close','volume','turnover']
            df = df.drop(['date'],1)
            print ('Got  NIFTY ticker data from CSV')
         
        return df   
        
    
    def getDataFromNSE(self,ticker, flag):
        
            mydata = nse.get_history(symbol=ticker, 
                        start=date(2016,1,1), 
                        end=date(2017,12,31),
    					index=True)
            df = pd.DataFrame(mydata)
            filename = 'datasets\\'+ticker+'.csv'
            
            df.to_csv(filename)
    #        df.columns = ['open', 'high','low','close','volume','turnover']
            df.columns = ['open', 'high','low','close','volume','turnover']
            print ('Got data from NSE')
    

    def moving_average(self, df):
        #MSE does not change using 4 below
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
#        df['macd_signal_diff'] = df['macd_9ema_signal']  - df['MACD']
        df = df.drop(['26ema', '12ema'],1 )
#        df = df.drop(['26ema', '12ema', 'MACD','macd_9ema_signal'],1 )
        
#        df.plot(y = ["MACD"], title = "MACD")
        return df  

    def addMACDAsFeature(self, df):
        
        mydf = self.MACD(df)
        mydf = self.calculateRSI(mydf)
        mydf['macd_0'] = mydf['MACD']
        mydf['macd_1'] = mydf['MACD'].shift(1)
        mydf['macd_2'] = mydf['MACD'].shift(2)
        mydf['macd_sig_0'] = mydf['macd_9ema_signal']       
        mydf['macd_sig_1'] = mydf['macd_9ema_signal'].shift(1)
 
        mydf['rsi0'] = mydf['rsi']
        mydf['rsi1'] = mydf['rsi'].shift(1)
        mydf['rsi2'] = mydf['rsi'].shift(2)
        
#        print ('mydf - ', mydf)
        mydf['diff_0'] = mydf['macd_0']  - mydf['macd_sig_0']
        mydf['diff_1'] = mydf['macd_1']  - mydf['macd_sig_1']
        
        pos_threshold = 0.3
        neg_threshold = -0.3
        '''
        mydf['macd_result'] = np.where((mydf['macd_0'] < 0) & (mydf['diff_0'] < 0 ) & ( mydf['diff_1'] < mydf['diff_0']) & (mydf['diff_0'] > neg_threshold)  , 'MACD-PBuy', 'MACD-PHold')
        mydf['macd_result'] = np.where((mydf['macd_result'] == "MACD-PHold") & (mydf['macd_0'] > 0) & (mydf['diff_0'] > 0 ) & ( mydf['diff_1'] > mydf['diff_0']) & (mydf['diff_0'] < pos_threshold)  , 'MACD-PSell', mydf['macd_result'])    
        mydf['macd_result'] = np.where((mydf['macd_0'] > mydf['macd_sig_0'] ) & (mydf['macd_1'] <= mydf['macd_sig_1'] ), 'MACD-Buy', mydf['macd_result'])    
        mydf['macd_result'] = np.where((mydf['macd_0'] < mydf['macd_sig_0'] ) & (mydf['macd_1'] >= mydf['macd_sig_1'] ), 'MACD-Sell', mydf['macd_result'])    
        
        mydf['rsi_result'] = np.where((mydf['rsi0'] < 70) & (mydf['rsi0'] > 50 ) & (mydf['rsi0'] < mydf['rsi1']) & (mydf['rsi1'] < mydf['rsi2']) & (mydf['rsi2'] > 70 )  , 'RSI-Sell', 'RSI-Hold')
        mydf['rsi_result'] = np.where((mydf['rsi0'] > 70) & (mydf['rsi0'] < mydf['rsi1']) & (mydf['rsi1'] < mydf['rsi2'])  , 'RSI-PSell', mydf['rsi_result'])
        mydf['rsi_result'] = np.where((mydf['rsi0'] > 30) & (mydf['rsi0'] < 50 ) & (mydf['rsi1'] < mydf['rsi0']) & (mydf['rsi2'] < mydf['rsi1']) & (mydf['rsi2'] < 30 )  , 'RSI-Buy', mydf['rsi_result'])
        mydf['rsi_result'] = np.where((mydf['rsi0'] < 30) & (mydf['rsi0'] > 20 ) & (mydf['rsi1'] < mydf['rsi0'])  , 'RSI-PBuy', mydf['rsi_result'])
        '''
        '''
        #sell=-2, Psell=-1, hold=0, pbuy=1, buy=2
        mydf['macd_result'] = np.where((mydf['macd_0'] < 0) & (mydf['diff_0'] < 0 ) & ( mydf['diff_1'] < mydf['diff_0']) & (mydf['diff_0'] > neg_threshold)  ,1, 0)
        mydf['macd_result'] = np.where((mydf['macd_result'] == 0) & (mydf['macd_0'] > 0) & (mydf['diff_0'] > 0 ) & ( mydf['diff_1'] > mydf['diff_0']) & (mydf['diff_0'] < pos_threshold)  , -1, mydf['macd_result'])    
        mydf['macd_result'] = np.where((mydf['macd_0'] > mydf['macd_sig_0'] ) & (mydf['macd_1'] <= mydf['macd_sig_1'] ), 2, mydf['macd_result'])    
        mydf['macd_result'] = np.where((mydf['macd_0'] < mydf['macd_sig_0'] ) & (mydf['macd_1'] >= mydf['macd_sig_1'] ), -2, mydf['macd_result'])    
        
        mydf['rsi_result'] = np.where((mydf['rsi0'] < 70) & (mydf['rsi0'] > 50 ) & (mydf['rsi0'] < mydf['rsi1']) & (mydf['rsi1'] < mydf['rsi2']) & (mydf['rsi2'] > 70 )  , -2, 0)
        mydf['rsi_result'] = np.where((mydf['rsi0'] > 70) & (mydf['rsi0'] < mydf['rsi1']) & (mydf['rsi1'] < mydf['rsi2'])  , -1, mydf['rsi_result'])
        mydf['rsi_result'] = np.where((mydf['rsi0'] > 30) & (mydf['rsi0'] < 50 ) & (mydf['rsi1'] < mydf['rsi0']) & (mydf['rsi2'] < mydf['rsi1']) & (mydf['rsi2'] < 30 )  , 2, mydf['rsi_result'])
        mydf['rsi_result'] = np.where((mydf['rsi0'] < 30) & (mydf['rsi0'] > 20 ) & (mydf['rsi1'] < mydf['rsi0'])  , 1, mydf['rsi_result'])
        '''

        #sell=-2, Psell=-1, hold=0, pbuy=1, buy=2
        mydf['macd_result'] = np.where((mydf['macd_0'] < mydf['macd_1'])  ,-1, 0) #sell
        mydf['macd_result'] = np.where((mydf['macd_0'] > mydf['macd_1']) ,1, mydf['macd_result']) #buy
        
        mydf['rsi_result'] = np.where((mydf['rsi0'] < mydf['rsi1'])   , -1, 0)
        mydf['rsi_result'] = np.where((mydf['rsi0'] > mydf['rsi1'])   , 1, mydf['rsi_result'])


#        print(mydf.columns)
        mydf = mydf.drop(['diff_0', 'diff_1', 'rsi0','rsi1','rsi2','macd_0','macd_1','macd_2','macd_sig_0','macd_sig_1','MACD', 'macd_9ema_signal', 'rsi'],1)
        filename = 'datasets\\mydf.csv'
#        mydf.to_csv(filename)
#        print(mydf.columns)
        return mydf
        
        
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
    
    def predictNextDayPrice(self):
        
        df = self.getData('NIFTY_50')
        
    def addChangeFromPreviousDay(self, df, feature, days):
        
        feature_prev_day = feature+'_prev_{}'
        feature_change = feature+'_change_{}'
        for i in range(1, days+1):
            new_feature = feature_prev_day.format(i)
            df[new_feature] = df[feature].shift(i)
            new_change_feature = feature_change.format(i)
            df[new_change_feature] = df[feature] - df[new_feature]
#            df = df.drop([new_feature],1 )
            
        return df
    
    def addNiftyCloseAsFeature(self, df, flag):
        temp = self.getNiftyData("NIFTY_50", flag)
        #Drop all column but 'close'
        temp = temp[['close']]
#        temp = temp[['close', 'Date']]
        temp=temp.rename(columns = {'close':'nifty_close'})
        #concate does an outerjoin.. this is case where some records in Nist proces missing
        # for few dates
        df = pd.concat([df, temp], axis=1)
#        #some nifty values are coming as NaN
#        df = df.fillna( df['nifty_close'].rolling(window=1).mean()) 
        return df
    
    def addOpenInterestData(self,df, ticker):
        oiDf = pd.read_csv('merged_csv.csv')
        oiDf = oiDf.loc[oiDf[' NSE Symbol'] == ticker]
        oiDf = oiDf[['Date',' NSE Open Interest']]
        oiDf.columns = ['date', 'open_interest']
        
        oiDf['date'] = pd.to_datetime(oiDf['date'])
        oiDf.set_index('date', inplace=True)
        oiDf.sort_index(axis=0, inplace=True, ascending=True)
        oiDf['prev_day_oi'] = oiDf['open_interest'].shift(1)
        oiDf['oi_change'] = oiDf['open_interest'] - oiDf['prev_day_oi']
        
#        df= df.merge(oiDf)
        df = df.join(oiDf)
#        df= df.merge(oiDf,  left_index=True, right_on='date')
        df = df.drop(['prev_day_oi' ],1 )
#        df = df.drop(['date_x','date_y','prev_day_oi' ],1 )
        print(df)
        return df
        
    def addOptionsANDFoData(self,df, ticker):
        print('processing OI data.......')
        interesting_files = glob.glob("./nse_downloads/*.csv") 
        fullDf = pd.DataFrame()
        for filename in interesting_files:
            oiDf = pd.read_csv(filename)
            oiDf = oiDf.loc[oiDf['SYMBOL'] == ticker]
            call_data = oiDf.loc[oiDf['OPTION_TYP'] == 'CE']
            put_data = oiDf.loc[oiDf['OPTION_TYP'] == 'PE']
            call_data_grouped = call_data.groupby(['TIMESTAMP']).agg({'CONTRACTS': sum, 'OPEN_INT': sum}).reset_index()
            call_data_grouped.columns = ['date','call_contracts', 'call_open_int']
            put_data_grouped = put_data.groupby(['TIMESTAMP']).agg({'CONTRACTS': sum, 'OPEN_INT': sum}).reset_index()
            put_data_grouped.columns = ['date','put_contracts', 'put_open_int']
            
#            frames = [call_data_grouped, put_data_grouped ]
#            combdf = pd.concat(frames)
            combdf = pd.merge(call_data_grouped, put_data_grouped, on='date')
            frames = [fullDf, combdf]
            fullDf= pd.concat(frames)
            print(fullDf.shape)
            
#        print(fullDf.head())
#        print(fullDf.tail())
        #sort of date so that shift from previous day can be calculated
        
        fullDf['date'] = pd.to_datetime(fullDf['date'])
        fullDf.set_index('date', inplace=True)
        fullDf.sort_index(axis=0, inplace=True, ascending=True)
        print(fullDf.head())
        print(fullDf.tail())
        df = df.join(fullDf)
        print('......DONE processing OI data.......')
        return df
        
        
    def addPrevDaysAsFeature(self, df,feature, days):
        feature_diff = feature+'_prev_{}'
        for i in range(1, days+1):
            new_feature = feature_diff.format(i)
            df[new_feature] = df[feature].shift(i)
            
        return df    

    def plot_graph(self,forecast_set, df):
        df['Forecast'] = np.nan
        last_date = df.iloc[-1].date
        last_date = datetime.datetime.strptime(last_date, "%m/%d/%Y")
        # last_unix = last_date.timestamp()
        last_unix = time.mktime(last_date.timetuple())
        #last_unix = last_date
        one_day = 86400
        next_unix = last_unix + one_day

        for i in forecast_set:
            next_date = datetime.datetime.fromtimestamp(next_unix)
            next_unix += one_day
            df.loc[next_date] = [np.nan for _ in range(len(df.columns) - 1)] + [i]
        print (df.head(n=10))
        print (df.tail(n=10))
        df['close'].plot()
        df['Forecast'].plot()
        plt.legend(loc=4)
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.show()    