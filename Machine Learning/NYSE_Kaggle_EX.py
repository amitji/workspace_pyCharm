import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import math, datetime, time

from sklearn import preprocessing, cross_validation, svm
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from matplotlib import style
import pickle

class NYSE_Kaggele_Ex:


    def execute(self):
        df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NYSE\\GOOG.csv")
        print df.columns

        df = self.moving_average(df)
        df = df.dropna(axis=0, how='any')
        forecast_out = int(math.ceil(0.01*len(df)))
        print(len(df))
        print(forecast_out)

        X = np.array(df.drop(['close', 'symbol', 'date','open','low','high'],1))
        #X = np.array(df.drop(['close', 'symbol', 'date', 'high', 'low'],1))
        #X = preprocessing.scale(X)
        print X[-5:]

        X_Validation = X[-forecast_out:]
        print X_Validation[-5:]

        X = X[:-forecast_out]
        print X[-5:]

        y = np.array(df['close'])
        y = y[:-forecast_out]
        #print y[-5:]

        print(len(X), len(y))


        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2)

        clf = LinearRegression(n_jobs=-1)
        # clf = svm.SVR(kernel='poly')

        clf.fit(X_train, y_train)

        accuracy = clf.score(X_test, y_test)
        print "\n\n****** Accuracy  - ",accuracy

        # df2 = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\NYSE\\GOOG_Validation.csv")
        # X_Validation = np.array(df2.drop(['close', 'symbol', 'date','open','low','high'],1))
        # #X_Validation = preprocessing.scale(X_Validation)
        # print X_Validation[-5:]



        forecast_set = clf.predict(X_Validation)
        print (forecast_set)

        df['Forecast'] = np.nan
        last_date = df.iloc[-1].name
        # last_unix = last_date.timestamp()
        last_unix = time.mktime(last_date.timetuple())
        one_day = 86400
        next_unix = last_unix + one_day

        for i in forecast_set:
            next_date = datetime.datetime.fromtimestamp(next_unix)
            next_unix += one_day
            df.loc[next_date] = [np.nan for _ in range(len(df.columns) - 1)] + [i]
        print (df.head(n=10))
        print (df.tail(n=10))
        df['Close'].plot()
        df['Forecast'].plot()
        plt.legend(loc=4)
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.show()

    def moving_average(self, df):
        df['50dma'] = df['close'].rolling(window=50).mean()
        df['100dma'] = df['close'].rolling(window=100).mean()
        df['5dma'] = df['close'].rolling(window=5).mean()

        return df


if __name__ == "__main__":
    c = NYSE_Kaggele_Ex()

    c.execute()