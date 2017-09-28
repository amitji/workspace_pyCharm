
import pandas as pd
import math, datetime, time
import quandl
import numpy as np
from sklearn import preprocessing, cross_validation, svm
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from matplotlib import style
import pickle


style.use('ggplot')

Authkey = '5_pRK9pKefuvZzHe-MkSy'

def test_Nifty():
    #df = quandl.get('WIKI/GOOGL', authtoken=Authkey)
    dfNifty = quandl.get("NSE/NIFTY_50", authtoken=Authkey)
    start_date = '2016-01-01'
    end_date = '2017-09-30'
    dates = pd.date_range(start_date, end_date)
    # print dates
    df = pd.DataFrame(index=dates)
    df = df.join(dfNifty, how='inner')

    dfTemp = pd.DataFrame(index=dates)
    df = df.join(dfTemp, how='outer')

    print df
    print df.columns


    df['HL_PCT'] = (df['High'] - df['Close'])/df['Close'] * 100
    df['PCT_change'] = (df['Close'] - df['Open'])/df['Open'] * 100

    df = df [['Close','HL_PCT','PCT_change', 'Shares Traded']]

    # print (df.head())
    # print (df.tail())

    forecast_col = 'Close'
    df.fillna(-99999,inplace=True )

    forecast_out = int(math.ceil(0.01*len(df)))
    print(len(df))
    print(forecast_out)
    #df['label'] = df[forecast_col].shift(-forecast_out)
    df['label'] = df[forecast_col]

    print (df.head(n=10))
    print (df.tail(n=10 ))

    X = np.array(df.drop(['label'],1))
    X = preprocessing.scale(X)
    print X
    X_lately = X[-forecast_out:]
    print X_lately
    X = X[:-forecast_out]
    print X

    # print(len(X))
    # print(len(X_lately))

    #df.dropna(inplace=True)
    y = np.array(df['label'])
    #y = np.array(df['label'])

    print(len(X), len(y))
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X,y,test_size=0.2)

    clf = LinearRegression(n_jobs=-1)
    #clf = svm.SVR(kernel='poly')
    clf.fit(X_train, y_train)
    with open('linerregression.pickel', 'wb') as f:
        pickle.dump(clf,f)

    pickle_in = open('linerregression.pickel', 'rb')
    clf = pickle.load(pickle_in)


    accuracy = clf.score(X_test, y_test)

    print(accuracy)

    forecast_set = clf.predict(X_lately)
    print (forecast_set)

    df['Forecast'] = np.nan
    last_date = df.iloc[-1].name
    #last_unix = last_date.timestamp()
    last_unix = time.mktime(last_date.timetuple())
    one_day = 86400
    next_unix = last_unix+one_day

    for i in forecast_set:
        next_date = datetime.datetime.fromtimestamp(next_unix)
        next_unix += one_day
        df.loc[next_date] = [np.nan for _ in range(len(df.columns)-1)] + [i]
    print (df.head(n=10))
    print (df.tail(n=10 ))
    df['Close'].plot()
    df['Forecast'].plot()
    plt.legend(loc=4)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()


def test_google():

    df = quandl.get('WIKI/GOOGL', authtoken=Authkey)
    #df = quandl.get("NSE/NIFTY_50", authtoken=Authkey)
    print df
    df = df [['Adj. Open', 'Adj. High','Adj. Low','Adj. Close','Adj. Volume']]
    df['HL_PCT'] = (df['Adj. High'] - df['Adj. Close'])/df['Adj. Close'] * 100
    df['PCT_change'] = (df['Adj. Close'] - df['Adj. Open'])/df['Adj. Open'] * 100

    df = df [['Adj. Close','HL_PCT','PCT_change', 'Adj. Volume']]

    print (df.head())
    print (df.tail())

    forecast_col = 'Adj. Close'
    df.fillna(-99999,inplace=True )

    forecast_out = int(math.ceil(0.01*len(df)))
    print(len(df))
    print(forecast_out)
    df['label'] = df[forecast_col].shift(-forecast_out)


    X = np.array(df.drop(['label'],1))
    X = preprocessing.scale(X)
    X_lately = X[-forecast_out:]
    X = X[:-forecast_out]


    # print(len(X))
    # print(len(X_lately))

    df.dropna(inplace=True)
    y = np.array(df['label'])
    #y = np.array(df['label'])

    print(len(X), len(y))
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X,y,test_size=0.2)

    clf = LinearRegression(n_jobs=-1)
    #clf = svm.SVR(kernel='poly')
    clf.fit(X_train, y_train)
    with open('linerregression.pickel', 'wb') as f:
        pickle.dump(clf,f)

    pickle_in = open('linerregression.pickel', 'rb')
    clf = pickle.load(pickle_in)


    accuracy = clf.score(X_test, y_test)

    print(accuracy)

    forecast_set = clf.predict(X_lately)
    print (forecast_set)

    df['Forecast'] = np.nan
    last_date = df.iloc[-1].name
    #last_unix = last_date.timestamp()
    last_unix = time.mktime(last_date.timetuple())
    one_day = 86400
    next_unix = last_unix+one_day

    for i in forecast_set:
        next_date = datetime.datetime.fromtimestamp(next_unix)
        next_unix += one_day
        df.loc[next_date] = [np.nan for _ in range(len(df.columns)-1)] + [i]
    print (df.head())
    print (df.tail(10))
    df['Adj. Close'].plot()
    df['Forecast'].plot()
    plt.legend(loc=4)
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.show()


if __name__ == "__main__":
    test_google()
    #test_Nifty()
