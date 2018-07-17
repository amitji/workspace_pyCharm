# -*- coding: utf-8 -*-
"""
Created on Sat Feb 17 19:39:58 2018

@author: aasgoyal
"""

###### DEFINE TICKER HERE ######
ticker = 'MARUTI'

import pandas as pd
import quandl

# Get Stock data

def get_data(ticker):
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    nse_dataset = "NSE" + "/" + ticker
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=700, sort_order="asc")
    df = pd.DataFrame(mydata)
    df.columns = ['open', 'high', 'low', 'last', 'close', 'volume', 'turnover']
    return df

stock_data = get_data(ticker)
stock_data['Date'] = stock_data.index

# Get NIFTY 50 data
nifty_data = pd.read_csv('nifty50.csv')
nifty_data['Date'] = pd.to_datetime(nifty_data['Date'])
nifty_data = nifty_data[['Date', 'nifty']]

# Stock wise F&O data
fo_file_path = 'aggregated_data_by_symbol/{}.csv'.format(ticker)
fo_data = pd.read_csv(fo_file_path) 
call_data = fo_data[fo_data['OPTION_TYP'] == 'CE']
put_data = fo_data[fo_data['OPTION_TYP'] == 'PE']
call_data['strike_contract'] = call_data['STRIKE_PR'] * call_data['CONTRACTS']
put_data['strike_contract'] = put_data['STRIKE_PR'] * put_data['STRIKE_PR']

call_data_needed = call_data[['TIMESTAMP', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'strike_contract']]
put_data_needed = put_data[['TIMESTAMP', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'strike_contract']]
call_data_grouped = call_data_needed.groupby(['TIMESTAMP']).agg({'CONTRACTS': sum, 'VAL_INLAKH': sum, 'OPEN_INT': sum, 'strike_contract': sum}).reset_index()
put_data_grouped = put_data_needed.groupby(['TIMESTAMP']).agg({'CONTRACTS': sum, 'VAL_INLAKH': sum, 'OPEN_INT': sum, 'strike_contract': sum}).reset_index()
data = call_data_grouped.merge(put_data_grouped, on='TIMESTAMP')
data.rename(columns={'TIMESTAMP':'Date'}, inplace=True)
data['Date'] = pd.to_datetime(data['Date'])

# combined OI data

oi_data = pd.read_csv('aggregated_oi.csv')
oi_data = oi_data[oi_data[' NSE Symbol']==ticker]
oi_data['Date'] = pd.to_datetime(oi_data['Date'])

# Final merges
#dataset = data.sort_values(by=['Date'])
#oi_data = oi_data.sort_values(by=['Date'])
dataset = data.merge(oi_data, on='Date')
dataset = dataset.merge(nifty_data, on='Date')
dataset = dataset.merge(stock_data, on='Date')
dataset.rename(columns={'CONTRACTS_x': 'call', 'CONTRACTS_y': 'put', 
                        'VAL_INLAKH_x': 'call_turn', 'VAL_INLAKH_y': 'put_turn', 
                        'OPEN_INT_x': 'call_oi', 'OPEN_INT_y': 'put_oi',
                        'strike_contract_x': 'call_strike', 
                        'strike_contract_y': 'put_strike',
                        ' NSE Open Interest': 'oi'}, inplace=True)

dataset = dataset[['Date', 'call', 'call_turn', 'call_oi', 'call_strike', 'put',
                   'put_turn', 'put_oi', 'put_strike', 'oi', 'nifty', 'close']]

dataset = dataset.sort_values(by=['Date'])

dataset['call_price'] = dataset['call_strike']/dataset['call']
dataset['put_price'] = dataset['put_strike']/dataset['put']
dataset['call_diff'] = dataset['call_price'] - dataset['close']
dataset['put_diff'] = dataset['close'] - dataset['put_price']

dataset['next_close'] = dataset['close'].shift(-1)
dataset['diff'] = dataset['next_close'] - dataset['close']

dataset['prev_oi'] = dataset['oi'].shift(1)
dataset['diff_oi'] = dataset['oi'] - dataset['prev_oi']

dataset['prev_close'] = dataset['close'].shift(1)
dataset['diff_close'] = dataset['close'] - dataset['prev_close']

dataset['prev_put_oi'] = dataset['put_oi'].shift(1)
dataset['diff_put_oi'] = dataset['put_oi'] - dataset['prev_put_oi']

dataset['prev_call_oi'] = dataset['call_oi'].shift(1)
dataset['diff_call_oi'] = dataset['call_oi'] - dataset['prev_call_oi']

dataset['prev_nifty'] = dataset['nifty'].shift(1)
dataset['diff_nifty'] = dataset['nifty'] - dataset['prev_nifty']

##############************##############

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

###############*********************############

dataset['output'] = dataset['diff'].apply(lambda x: 0 if x < 0 else 1)


dataset['rsi_prediction'] = dataset.apply(potential_buy_sell_call_rsi, axis=1)
dataset['macd_prediction'] = dataset.apply(potential_buy_sell_call_macd, axis=1)

dataset['pcr_turn'] = dataset['put_turn']/dataset['call_turn']

dataset['direct_signal'] = (dataset['put_diff'] - dataset['call_diff']).apply(lambda x: 0 if x < 0 else 1)

dataset['oi_output'] = dataset['diff_oi'].apply(lambda x: 0 if x < 0 else 1)

dataset['put_oi_output'] = dataset['diff_put_oi'].apply(lambda x: 0 if x < 0 else 1)

dataset['call_oi_output'] = dataset['diff_call_oi'].apply(lambda x: 0 if x < 0 else 1)

dataset['nifty_output'] = dataset['diff_nifty'].apply(lambda x: 0 if x < 0 else 1)

dataset['diff_output'] = dataset['diff_close'].apply(lambda x: 0 if x < 0 else 1)

###############*********************############

dataset['dma_1'] = dataset['close'].rolling(window=5, min_periods=1).mean() - dataset['close']
dataset['dma_2'] = dataset['close'].rolling(window=10, min_periods=1).mean() - dataset['close']
dataset['dma_3'] = dataset['close'].rolling(window=20, min_periods=1).mean() - dataset['close']


for i in range(0, 10):
    name = 'prev_oi_{}'.format(i)
    dataset[name] = dataset['diff_oi'].shift(i)
    name = 'prev_close_{}'.format(i)
    dataset[name] = dataset['diff_close'].shift(i)
    name = 'prev_nifty_{}'.format(i)
    dataset[name] = dataset['diff_nifty'].shift(i)
    name = 'prev_put_oi_{}'.format(i)
    dataset[name] = dataset['diff_put_oi'].shift(i)
    name = 'prev_call_oi_{}'.format(i)
    dataset[name] = dataset['diff_call_oi'].shift(i)
    name = 'prev_call_strike_signal_{}'.format(i)
    dataset[name] = dataset['direct_signal'].shift(i)


dataset = dataset[10:-1]


#############


X = dataset.iloc[:, 42:].values
y = dataset.iloc[:, 41].values

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder_X_1 = LabelEncoder()
X[:, 0] = labelencoder_X_1.fit_transform(X[:, 0])
labelencoder_X_2 = LabelEncoder()
X[:, 1] = labelencoder_X_2.fit_transform(X[:, 1])
onehotencoder = OneHotEncoder(categorical_features = [0, 1])
X = onehotencoder.fit_transform(X).toarray()
X = X[:, [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 
          21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 
          38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 
          55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 
          72, 73]]

## Random forest classifier

from sklearn.model_selection import train_test_split, cross_val_score
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

from sklearn.ensemble import RandomForestClassifier

classifier = RandomForestClassifier(n_estimators=22, random_state=0)
classifier.fit(X_train, y_train)
scores = cross_val_score(classifier, X, y, cv=20)
score = sum(scores) / 20
print(score)
y_pred = classifier.predict(X_test)


## ANN classifier
cm_values = list()
for i in range(10):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    from sklearn.preprocessing import StandardScaler
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)
    
    from keras.models import Sequential
    from keras.layers import Dense
    # Initialising the ANN
    classifier = Sequential()
    
    # Adding the input layer and the first hidden layer
    classifier.add(Dense(output_dim = 20, init = 'uniform', activation = 'relu', input_dim = 72))
    
    # Adding the second hidden layer
    classifier.add(Dense(output_dim = 5, init = 'uniform', activation = 'relu'))
    

    # Adding the output layer
    classifier.add(Dense(output_dim = 1, init = 'uniform', activation = 'hard_sigmoid'))
    
    # Compiling the ANN
    classifier.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])
    
    # Fitting the ANN to the Training set
    classifier.fit(X_train, y_train, batch_size = 10, nb_epoch = 10)
    
    # Part 3 - Making the predictions and evaluating the model
    
    # Predicting the Test set results
    y_pred = classifier.predict(X_test)
    y_pred_o = (y_pred > 0.5)
    
    # Making the Confusion Matrix
    from sklearn.metrics import confusion_matrix
    cm = confusion_matrix(y_test, y_pred_o)
    acc = (cm[0][0]+cm[1][1])/(cm[0][0]+cm[1][1]+cm[1][0]+cm[0][1])
    cm_values.append(acc)
    
sum(cm_values)/len(cm_values)
