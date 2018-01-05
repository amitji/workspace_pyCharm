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
import StockPredictionHelper

class Nifty_Regression_Ex:

    def __init__(self):
        self.helper = StockPredictionHelper.StockPredictionHelper()
        
        
    def backTesting(self, ticker, flag):
        print("\n\n\n\n\n***********  New Run of Nifty_Regression_Ex **********************\n\n")
        pd.set_option('display.expand_frame_repr', False)
#        np.set_printoptions(formatter={'float_kind':'{:f}'.format})
        np.set_printoptions(suppress=True)
        

        df = self.helper.getData(ticker, flag)
#        df = self.helper.getDataFromNSE(ticker, flag)
        
        #fil NaN data with average
        df = df.fillna(df.mean())
#        print ("Top 5 - ", df.head(5))
#        print ("Last 5 - ", df.tail(5))
        
        #Just take some X years of data if not all years
        #df = df[1259:]
        

        
        # Add another column next_day_close exct duplicate of close. Later we shift to make prev close.
        # First shift 'close' by -1 because today's close depends on all the feature values of yesterday. To predict today's close
        # we need the open, high, lo, volumne of yesterday because we dont have these values yet for today ! 
        df['next_day_close'] = df['close'].shift(-1)
        
        #add change & change % from previous day
        df = self.helper.addChangeFromPreviousDay(df)
#        df = self.helper.addNiftyCloseAsFeature(df, flag)
        
        # Add few Moving avgs to dataframe
        df = self.helper.moving_average(df)
        df = self.helper.MACD(df)
        df = df.dropna(axis=0, how='any')          
#        df = df[:-1] # remove last record
        
        
#        print ("Last 5 records - ", df.tail(5))
        
        
#        X = np.array(df.drop(['next_day_close','open','low','high', 'turnover'],1))
        X_df = df.drop(['next_day_close','open','low','high', 'turnover'],1)
        
        #X = np.array(df.drop(['close', 'symbol', 'date'],1))
        print ("DF Columns - ",X_df.columns)
       
        X = np.array(X_df)
        X = preprocessing.scale(X)
#        X = preprocessing.normalize(X)
        
#        print ("After preprocessing- ", X[-5:])

        y = np.array(df['next_day_close'])
        print ("Y values - ", y[-5:])

#        print("Dataset lenghts - ",(len(X), len(y)))


        #X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.3)
        #train, validate, test = np.split(df.sample(frac=1), [int(.6*len(df)), int(.8*len(df))])
        X_train, X_rest, y_train, y_rest = model_selection.train_test_split(X, y, test_size=0.4, random_state=42)
        X_test, X_Validation, y_test, y_Validation = model_selection.train_test_split(X_rest, y_rest, test_size=0.5, random_state=42)
        
#        print("Dataset lenghts Train set- ",(len(X_train), len(y_train)))
#        print("Dataset lenghts Test set- ",(len(X_test), len(y_test)))
#        print("Dataset lenghts validation set- ",(len(X_Validation), len(y_Validation)))
        
        #result_df = pd.DataFrame()
#        result_df.columns = [['Actual Close', 'Linear Reg', 'SVM', 'Decision Tree']]
#        self.result_df.loc[0, 'Actual Close'] = X_Validation[0]

#        df.loc[df.shape[0]] = ['d', 3] 
        
        
#        self.callLinearRegression(X_train, X_test, y_train, y_test,X_Validation, y_Validation )
#        self.callLogisticRegression(X_train, X_test, y_train, y_test,X_Validation , y_Validation )
#        self.callSVM(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
#        self.callDecisionTreeRegressor(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
        self.callRandomForest(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
#        self.callGradientBoosting(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
#        self.callXGBClassifier(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
#        self.callKNN(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
    
#        print (self.result_df)
        
    def callLinearRegression(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation):
        # Linear regression
        clf = LinearRegression(n_jobs=-1)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        
        y_predicted = clf.predict(X_Validation)
#        accuracy = clf.score(X_Validation, y_Validation )
#        print ("\n\n****** LinearRegression Accuracy  - ",accuracy)
        
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        
#        plt.scatter(y_Validation, y_predicted)
#        plt.xlabel('True Values')
#        plt.ylabel('Predictions')
        print ("\n****** LinearRegression Accuracy  - ",accuracy)
        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)
        
        
    def callDecisionTreeRegressor(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation):
        #Decision Tree
        clf = tree.DecisionTreeRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** Decision Tree Accuracy  - ",accuracy)  
        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)


#        self.result_df.loc[0, 'DecisionTree'] = forecast_set  

    def callRandomForest(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation ):
        #Random forest
        clf = RandomForestRegressor(min_samples_leaf=1, max_features=0.5, n_estimators=20, random_state=42, oob_score=True)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** RandomForestClassifier Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)

#        self.result_df.loc[0, 'RandomForest'] = forecast_set 
        
    def callGradientBoosting(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation):
        #GradientBoostingClassifier
        clf= GradientBoostingRegressor(n_estimators=100, learning_rate=1.0, max_depth=1, random_state=0)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** callGradientBoosting Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
#        self.result_df.loc[0, 'GradientBoosting'] = forecast_set 
        

    def callKNN(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation):
         #KNN
        clf = KNeighborsRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** KNeighborsRegressor Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
        
        # self.plot_graph(forecast_set, df)
        
    def callXGBClassifier(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation):
        #XGBClassifier
        clf = XGBRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** XGBRegressor Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
        

    def callLogisticRegression(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation ):
        # Linear regression
        clf = LogisticRegression(penalty='l2',C=.01)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** LogisticRegression Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
        
    def callSVM(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation ):
        # SVM        
        clf = svm.SVR(kernel='poly')
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print ("\n****** SVM Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        

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
    thisObj = Nifty_Regression_Ex()
    #MARUTI, AUROPHARMA
    ticker = 'MARUTI'
    thisObj.backTesting(ticker, 'real_time')
#    thisObj.backTesting(ticker, 'csv')
    
#    thisObj.predictNextDayPrice()

    print ("Done !!")