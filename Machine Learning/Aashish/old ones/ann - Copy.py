# -*- coding: utf-8 -*-
"""
Created on Sun Feb  4 14:37:45 2018

@author: agoy16
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 00:07:13 2018

@author: agoy16
"""

from keras.models import Sequential
from keras.layers import Dense

import pandas as pd
from sklearn.model_selection import cross_val_score, train_test_split
import quandl

score_dict = dict()


def get_data(ticker):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    nse_dataset = "NSE" + "/" + ticker
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=500, sort_order="asc")
    df = pd.DataFrame(mydata)
    df.columns = ['open', 'high', 'low', 'last', 'close', 'volume', 'turnover']
    return df

dataset = get_data('INFY')

def potential_buy_sell_call_rsi(row):
    rsi_prediction = 'Hold'
    if row['rsi_0'] < 70 and row['rsi_0'] > 50 and row['rsi_0'] < row['rsi_1'] and row['rsi_1'] < row['rsi_2'] and row[
        'rsi_2'] > 70:
        rsi_prediction = 'Sell'
    elif row['rsi_0'] > 70 and row['rsi_0'] < row['rsi_1'] and row['rsi_1'] < row['rsi_2']:
        rsi_prediction = 'Potential Sell'
    elif row['rsi_0'] > 30 and row['rsi_0'] < 50 and row['rsi_1'] < row['rsi_0'] and row['rsi_2'] < row['rsi_1'] and row[
        'rsi_2'] < 30:
        rsi_prediction = 'Buy'
    elif row['rsi_0'] < 30 and row['rsi_0'] > 20 and row['rsi_1'] < row['rsi_0']:
        rsi_prediction = 'Potential Buy'

    return rsi_prediction


def potential_buy_sell_call_macd(row):
    pos_threshold = 0.3
    neg_threshold = -0.3
    macd_prediction = 'Hold'
    if row['macd_0'] < 0 and row['macd_signal_diff_0'] < 0 and row['macd_signal_diff_1'] < row[
        'macd_signal_diff_0'] and (neg_threshold < row['macd_signal_diff_0'] < 0):
        macd_prediction = 'Potential Buy'

    elif row['macd_0'] > 0 and row['macd_signal_diff_0'] > 0 and row['macd_signal_diff_1'] > row[
        'macd_signal_diff_0'] and (0 < row['macd_signal_diff_0'] < pos_threshold):
        macd_prediction = 'Potential Sell'

    if row['macd_0'] > row['signal_line_0'] and row['macd_1'] <= row['signal_line_1']:
        macd_prediction = 'Buy'

    elif row['macd_0'] < row['signal_line_0'] and row['macd_1'] >= row['signal_line_1']:
        macd_prediction = 'Sell'

    return macd_prediction

def rsi(df):
    n = 14
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

dataset = macd(dataset)

dataset = rsi(dataset)

dataset['signal_line'] = dataset['macd'].ewm(span=9).mean()

dataset['next_close'] = dataset['close'].shift(-1)

dataset['macd_signal_diff'] = dataset['macd'] - dataset['signal_line']

dataset['macd_0'] = dataset['macd'].shift(0)
dataset['macd_1'] = dataset['macd'].shift(1)

dataset['signal_line_0'] = dataset['signal_line'].shift(0)
dataset['signal_line_1'] = dataset['signal_line'].shift(1)

dataset['macd_signal_diff_0'] = dataset['macd_signal_diff'].shift(0)
dataset['macd_signal_diff_1'] = dataset['macd_signal_diff'].shift(1)

dataset['rsi_0'] = dataset['rsi'].shift(0)
dataset['rsi_1'] = dataset['rsi'].shift(1)
dataset['rsi_2'] = dataset['rsi'].shift(2)


def cci(data, ndays): 
    tp = (data['high'] + data['low'] + data['close']) / 3 
    cci = pd.Series((tp - tp.rolling(window=ndays, min_periods=1).mean()) / (0.015 * tp.rolling(window=ndays, min_periods=1).std()),
                    name = 'cci') 
    data = data.join(cci) 
    return data

def emv(data, ndays): 
    dm = ((data['high'] + data['low'])/2) - ((data['high'].shift(1) + data['low'].shift(1))/2)
    br = (data['volume'] / 100000000) / ((data['high'] - data['low']))
    emv = dm / br 
    emv_ma = pd.Series(emv.rolling(window=ndays, min_periods=1).mean(), name = 'emv') 
    data = data.join(emv_ma) 
    return data 

def roc(data,n):
    N = data['close'].diff(n)
    D = data['close'].shift(n)
    roc = pd.Series(N/D,name='roc')
    data = data.join(roc)
    return data

def force_index(data, ndays): 
    fi = pd.Series(data['close'].diff(ndays) * data['volume'], name = 'ForceIndex') 
    data = data.join(fi) 
    return data

# Compute the Force Index for AAPL
n_fi = 1
dataset = force_index(dataset, n_fi)

# Compute the 5-period Rate of Change for NIFTY
n_roc = 5
dataset = roc(dataset, n_roc)

# Compute the 14-day Ease of Movement for AAPL
n_emv = 14
dataset = emv(dataset, n_emv)

# Compute the Commodity Channel Index(CCI) for NIFTY based on the 20-day Moving average
n_cci = 20
dataset = cci(dataset, n_cci)

dataset['divergence'] = dataset['close'] - dataset['macd']

dataset['next_roc'] = dataset['roc'].shift(1)
dataset['next_fi'] = dataset['ForceIndex'].shift(1)
dataset['next_divergence'] = dataset['divergence'].shift(1)

dataset['roc_diff'] = dataset['next_roc'] - dataset['roc']
dataset['roc_output'] = dataset['roc_diff'].apply(lambda x: 0 if x <0 else 1)

dataset['fi_diff'] = dataset['next_fi'] - dataset['ForceIndex']
dataset['fi_output'] = dataset['fi_diff'].apply(lambda x: 0 if x <0 else 1)

dataset['macd_diff'] = dataset['macd'] - dataset['signal_line']
dataset['macd_output'] = dataset['macd_diff'].apply(lambda x: 0 if x <0 else 1)

dataset['divergence_diff'] = dataset['next_divergence'] - dataset['divergence']
dataset['divergence_output'] = dataset['divergence_diff'].apply(lambda x: 0 if x <0 else 1)

dataset['rsi_prediction'] = dataset.apply(potential_buy_sell_call_rsi, axis=1)
dataset['macd_prediction'] = dataset.apply(potential_buy_sell_call_macd, axis=1)

dataset['diff'] = dataset['next_close'] - dataset['close']
dataset['output'] = dataset['diff'].apply(lambda x: 0 if x < 0 else 1)

dataset = dataset[16:-1]

X = dataset.iloc[:, [37, 38]].values
y = dataset.iloc[:, 40].values

# Splitting the dataset into the Training set and Test set

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder_X_1 = LabelEncoder()
X[:, 0] = labelencoder_X_1.fit_transform(X[:, 0])
labelencoder_X_2 = LabelEncoder()
X[:, 1] = labelencoder_X_2.fit_transform(X[:, 1])
onehotencoder = OneHotEncoder(categorical_features = [0, 1])
X = onehotencoder.fit_transform(X).toarray()
X = X[:, [1, 2, 3, 4, 6, 7, 8, 9]]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

# Initialising the ANN
classifier = Sequential()

# Adding the input layer and the first hidden layer
classifier.add(Dense(output_dim = 5, init = 'uniform', activation = 'relu', input_dim = 8))

# Adding the second hidden layer
classifier.add(Dense(output_dim = 5, init = 'uniform', activation = 'relu'))

# Adding the output layer
classifier.add(Dense(output_dim = 1, init = 'uniform', activation = 'sigmoid'))

# Compiling the ANN
classifier.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])

# Fitting the ANN to the Training set
classifier.fit(X_train, y_train, batch_size = 10, nb_epoch = 100)

# Part 3 - Making the predictions and evaluating the model

# Predicting the Test set results
y_pred = classifier.predict(X_test)
y_pred_o = (y_pred > 0.5)

# Making the Confusion Matrix
from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred_o)