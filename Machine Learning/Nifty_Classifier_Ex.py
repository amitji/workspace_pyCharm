import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import math, datetime, time

from sklearn import preprocessing, cross_validation, svm, model_selection
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn import tree
from sklearn.neighbors import KNeighborsClassifier,KNeighborsRegressor
from xgboost import XGBClassifier,XGBRegressor
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from matplotlib import style
import GetStockMarketData as stockdata
from sklearn.metrics import confusion_matrix

class Nifty_Classifier_Ex:
    def __init__(self):
#        self.result_df = [] 
        self.result_df = pd.DataFrame()
        
        
    def moving_average(self, df):
        df['5dma'] = df['close'].rolling(window=5).mean()
        df['20dma'] = df['close'].rolling(window=20).mean()
        df['50dma'] = df['close'].rolling(window=50).mean()
        df['100dma'] = df['close'].rolling(window=100).mean()
        #df['200dma'] = df['close'].rolling(window=200).mean()

        return df    

    def MACD(self, df):
        
#        df['26ema'] = pd.ewma(df["close"], span=26)
        df['26ema'] = df['close'].ewm(span=26).mean()
#        df['12ema'] = pd.ewma(df["close"], span=12)
        df['12ema'] = df['close'].ewm(span=12).mean()
        
        df['MACD'] = df['12ema'] - df['26ema']
        
        df = df.drop(["26ema", "12ema"],1 )
#        df.plot(y = ["MACD"], title = "MACD")
        return df  
    
    def execute(self):
        print("\n\n\n\n\n***********  New Run of Nifty_Classifier_Ex **********************\n\n")
        pd.set_option('display.expand_frame_repr', False)
        
#        df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NYSE\\GOOG.csv")
        df = stockdata.getData('NIFTY_50')
        
        #fil NaN data with average
        df = df.fillna(df.mean())
        print (df)
        
        #Just take some X years of data if not all years
        #df = df[1259:]
        
        #For backtesting, remove n records from bottom. Comment it if all records needed.
        n=2
        df = df[:-n]
        
        # Add another column next_day_close exct duplicate of close. Later we shift to make prev close.
        df['next_day_close'] = df['close'].shift(-1)
        df = df[:-1]
        df['diff'] = df['next_day_close'] - df['close']    
        df['output'] = df['diff'].apply(lambda x: 0 if x <0 else 1)

        # Add few Moving avgs to dataframe
        #df = self.moving_average(df)
        df = self.MACD(df)
#        print (df)
        df = df.dropna(axis=0, how='any')       
        
        print ("Last 5 records - ", df.tail(5))
                

        X = np.array(df.drop(['next_day_close','open','low','high', 'diff','output'],1))
        #X = np.array(df.drop(['close', 'symbol', 'date'],1))
        print ("DF Columns - ",df.columns)
       
        X = preprocessing.scale(X)
#        X = preprocessing.normalize(X)
        
        print ("After preprocessing- ", X[-5:])


        y = np.array(df['output'])

        print ("Y values - ", y[-5:])

        print("Dataset lenghts - ",(len(X), len(y)))


        #X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.3)
        X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.3, random_state=42)
        
        
#        self.callLinearRegression(X_train, X_test, y_train, y_test)
#        self.callLogisticRegression(X_train, X_test, y_train, y_test)
#        self.callSVM(X_train, X_test, y_train, y_test)
        self.callDecisionTreeRegressor(X_train, X_test, y_train, y_test )
        self.callRandomForest(X_train, X_test, y_train, y_test )
        self.callGradientBoosting(X_train, X_test, y_train, y_test )
        self.callXGBClassifier(X_train, X_test, y_train, y_test )
#        self.callKNN(X_train, X_test, y_train, y_test )
    
        print (self.result_df)
 
    def callRandomForest(self, X_train, X_test, y_train, y_test):
        clf = RandomForestClassifier(n_estimators=100, random_state=0)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        cm = confusion_matrix(y_test, y_pred)
        
        print (cm)

        plt.imshow(cm, cmap='binary', interpolation='None')
        plt.show()
        
        print (y_test- y_pred)
        plt.plot(y_test, y_pred)
#        x_max = len(y_test)
#        plt.axis([0,x_max,0,1])
        
    def callLinearRegression(self, X_train, X_test, y_train, y_test):
        # Linear regression
        clf = LinearRegression(n_jobs=-1)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
#        print ("\n\n****** LinearRegression Accuracy  - ",accuracy)
        forecast_set = clf.predict(X_test)
#        print ("X_Validation_Before_PreProcessing - ", X_Validation_Before_PreProcessing)
#        print ("LinearRegression forecast Values - ", forecast_set)
#        self.result_df.loc[0, 'Linear Reg'] = forecast_set     
        mse = mean_squared_error(y_test, forecast_set)
        variance = np.var(forecast_set) 
        
        print ("Varinace - ", variance)

        
    def callLogisticRegression(self, X_train, X_test, y_train, y_test):
        # Linear regression
        clf = LogisticRegression(penalty='l2',C=.01)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
#        print ("\n\n****** LogisticRegression Accuracy  - ",accuracy)
        forecast_set = clf.predict(X_Validation)
#        print ("X_Validation_Before_PreProcessing - ", X_Validation_Before_PreProcessing)
#        print ("LogisticRegression forecast Values - ", forecast_set)
        self.result_df.loc[0, 'Logistic Reg'] = forecast_set  
        
    def callSVM(self, X_train, X_test, y_train, y_test):
        # SVM        
        clf = svm.SVR(kernel='poly')
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
#        print ("\n\n****** SVM Accuracy  - ",accuracy)
        forecast_set = clf.predict(X_Validation)
#        print ("X_Validation_Before_PreProcessing - ", X_Validation_Before_PreProcessing)
#        print ("SVM forecast Values - ", forecast_set)
        self.result_df.loc[0, 'SVM'] = forecast_set  
        
    def callDecisionTreeRegressor(self, X_train, X_test, y_train, y_test ):
        #Decision Tree
        clf = tree.DecisionTreeClassifier()
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        cm = confusion_matrix(y_test, y_pred)
        
        print (cm)

        plt.imshow(cm, cmap='binary', interpolation='None')
        plt.show()
        
        print (y_test- y_pred)
        plt.plot(y_test, y_pred)
        
    def callGradientBoosting(self, X_train, X_test, y_train, y_test):
        #GradientBoostingClassifier
        clf= GradientBoostingClassifier(n_estimators=100, learning_rate=1.0, max_depth=1, random_state=0)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        cm = confusion_matrix(y_test, y_pred)
        
        print (cm)

        plt.imshow(cm, cmap='binary', interpolation='None')
        plt.show()
        
        print (y_test- y_pred)
        plt.plot(y_test, y_pred)
        

    def callKNN(self, X_train, X_test, y_train, y_test):
         #KNN
        clf = KNeighborsRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
#        print ("\n\n****** KNeighborsRegressor Tree Accuracy  - ",accuracy)
        forecast_set = clf.predict(X_Validation)
#        print ("X_Validation_Before_PreProcessing - ", X_Validation_Before_PreProcessing)
#        print ("KNN forecast Values - ", forecast_set)
        self.result_df.loc[0, 'KNN'] = forecast_set 
        
        # self.plot_graph(forecast_set, df)
        
    def callXGBClassifier(self, X_train, X_test, y_train, y_test):
        #XGBClassifier
        clf = XGBClassifier()
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        cm = confusion_matrix(y_test, y_pred)
        
        print (cm)

        plt.imshow(cm, cmap='binary', interpolation='None')
        plt.show()
        
        print (y_test- y_pred)
        plt.plot(y_test, y_pred)
        


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




if __name__ == "__main__":
    c = Nifty_Classifier_Ex()

    c.execute()

    print ("Done !!")# -*- coding: utf-8 -*-

