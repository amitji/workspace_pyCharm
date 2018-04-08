import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from sklearn import preprocessing, cross_validation, svm, model_selection
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_squared_error
import StockPredictionHelper

class Nifty_Regression_Ex:

    def __init__(self):
        self.helper = StockPredictionHelper.StockPredictionHelper()
        
        
    def backTesting(self, ticker, flag):
        print("\n\n\n\n\n***********  New Run of Nifty_Regression_Ex **********************\n\n")
        pd.set_option('display.expand_frame_repr', False)
        np.set_printoptions(suppress=True)
        

        df = self.helper.getData(ticker, flag)
        
        #fil NaN data with average
        df = df.fillna(df.mean())
        df = df.drop(['volume','open','low','high', 'turnover', 'date'],1)
        df['prev_day_close'] = df['close'].shift(1)
        df = df.dropna(axis=0, how='any')  
        X_df = df.drop(['close'],1)
        
        X = np.array(X_df)
        X = preprocessing.scale(X)
        
        y = np.array(df['close'])

        X_train, X_rest, y_train, y_rest = model_selection.train_test_split(X, y, test_size=0.4, random_state=42)
        X_test, X_Validation, y_test, y_Validation = model_selection.train_test_split(X_rest, y_rest, test_size=0.5, random_state=42)
        
        self.callRandomForest(X_train, X_test, y_train, y_test,X_Validation , y_Validation)
        

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


if __name__ == "__main__":
    thisObj = Nifty_Regression_Ex()
    #MARUTI, AUROPHARMA
    ticker = 'MARUTI'
    thisObj.backTesting(ticker, 'real_time')

    print ("Done !!")