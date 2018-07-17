# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 16:39:03 2018

@author: agoy16
"""

# -*- coding: utf-8 -*-

# Importing the libraries
import pandas as pd
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.ensemble import RandomForestClassifier
import quandl

score_dict = dict()

def get_data(ticker):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    nse_dataset = "NSE" + "/" + ticker
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=500, sort_order="asc")
    df = pd.DataFrame(mydata)
    df.columns = ['open', 'high', 'low', 'last', 'close', 'volume', 'turnover']
    return df

def rsi(df):
    n=14
    delta = df['close'].diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0
    
    RolUp = pd.rolling_mean(dUp, n)
    RolDown = pd.rolling_mean(dDown, n).abs()
    
    RS = RolUp / RolDown
    RSI = 100.0 - (100.0 / (1.0 + RS))
    df['rsi'] = RSI
    return df


def macd(data):
    data['26ema'] = data['close'].ewm(span=26).mean()
    data['12ema'] = data['close'].ewm(span=12).mean()
    data['macd'] = data['12ema'] - data['26ema']
    data = data.drop(["26ema", "12ema"], 1)
    return data

def cci(data, ndays):
    tp = (data['high'] + data['low'] + data['close']) / 3
    cci = pd.Series(
        (tp - tp.rolling(window=ndays, min_periods=1).mean()) / (0.015 * tp.rolling(window=ndays, min_periods=1).std()),
        name='cci')
    data = data.join(cci)
    return data


def emv(data, ndays):
    dm = ((data['high'] + data['low']) / 2) - ((data['high'].shift(1) + data['low'].shift(1)) / 2)
    br = (data['volume'] / 100000000) / ((data['high'] - data['low']))
    emv = dm / br
    emv_ma = pd.Series(emv.rolling(window=ndays, min_periods=1).mean(), name='emv')
    data = data.join(emv_ma)
    return data


def roc(data, n):
    N = data['close'].diff(n)
    D = data['close'].shift(n)
    roc = pd.Series(N / D, name='roc')
    data = data.join(roc)
    return data


def force_index(data, ndays):
    fi = pd.Series(data['close'].diff(ndays) * data['volume'], name='ForceIndex')
    data = data.join(fi)
    return data

dataset = get_data('INFY')

n_fi = 1
dataset = force_index(dataset, n_fi)

# Compute the 5-period Rate of Change for NIFTY
n_roc = 5
dataset = roc(dataset, n_roc)

dataset = macd(dataset)

dataset = rsi(dataset)

dataset['signal_line'] = dataset['macd'].ewm(span=9).mean()
dataset['divergence'] = dataset['close'] - dataset['macd']

dataset['next_close'] = dataset['close'].shift(-1)
dataset['next_roc'] = dataset['roc'].shift(1)
dataset['next_fi'] = dataset['ForceIndex'].shift(1)
dataset['next_divergence'] = dataset['divergence'].shift(1)
dataset = dataset[:-1]
dataset['diff'] = dataset['next_close'] - dataset['close']
dataset['output'] = dataset['diff'].apply(lambda x: 0 if x < 0 else 1)

dataset['roc_diff'] = dataset['next_roc'] - dataset['roc']
dataset['roc_output'] = dataset['roc_diff'].apply(lambda x: 0 if x < 0 else 1)

dataset['fi_diff'] = dataset['next_fi'] - dataset['ForceIndex']
dataset['fi_output'] = dataset['fi_diff'].apply(lambda x: 0 if x < 0 else 1)

dataset['macd_diff'] = dataset['macd'] - dataset['signal_line']
dataset['macd_output'] = dataset['macd_diff'].apply(lambda x: 0 if x < 0 else 1)

dataset['divergence_diff'] = dataset['next_divergence'] - dataset['divergence']
dataset['divergence_output'] = dataset['divergence_diff'].apply(lambda x: 0 if x < 0 else 1)

for i in range(0, 60):
    close_diff = 'close_diff_{}'.format(i)
    dataset[close_diff] = dataset['close'].shift(i)

    
#    name_div = 'divergence_diff_{}'.format(i)
#    dataset[name_div] = dataset['divergence_diff'].shift(i)
#    
#    name_macd = 'macd_diff_{}'.format(i)
#    dataset[name_macd] = dataset['macd_diff'].shift(i)
#    
#    name_fi = 'fi_diff_{}'.format(i)
#    dataset[name_fi] = dataset['fi_diff'].shift(i)
#    
#    name_roc = 'roc_diff_{}'.format(i)
#    dataset[name_roc] = dataset['roc_diff'].shift(i)

dataset = dataset[60:]

X = dataset.iloc[:, 10: ].values
y = dataset.iloc[:, 9].values

# Splitting the dataset into the Training set and Test set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

from sklearn.naive_bayes import GaussianNB
classifier = GaussianNB()
#classifier = RandomForestClassifier(n_estimators=200, random_state=0)
classifier.fit(X_train, y_train)

y_pred = classifier.predict(X_test)

from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)

scores = cross_val_score(classifier, X, y, cv=10)
score = sum(scores) / 10
print(score)
