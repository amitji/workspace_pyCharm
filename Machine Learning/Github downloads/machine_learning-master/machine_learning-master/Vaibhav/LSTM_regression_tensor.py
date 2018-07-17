# -*- coding: utf-8 -*-
"""
Created on Sat Feb 17 19:39:58 2018

@author: aasgoyal
"""

###### DEFINE TICKER HERE ######
import time
start_time = time.time()

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
nifty_data = pd.read_csv('data/nifty50.csv')
nifty_data['Date'] = pd.to_datetime(nifty_data['Date'])
nifty_data = nifty_data[['Date', 'nifty']]

# Stock wise F&O data
fo_file_path = 'data/{}.csv'.format(ticker)
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

oi_data = pd.read_csv('data/aggregated_oi.csv')
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

# dataset = dataset[['Date', 'call', 'call_turn', 'call_oi', 'call_strike', 'put',
#                   'put_turn', 'put_oi', 'put_strike', 'oi', 'nifty', 'close']]

dataset = dataset.sort_values(by=['Date'])

dataset['call_price'] = dataset['call_strike']/dataset['call']
dataset['put_price'] = dataset['put_strike']/dataset['put']
dataset['call_diff'] = dataset['call_price'] - dataset['close']
dataset['put_diff'] = dataset['close'] - dataset['put_price']


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

#dataset['output'] = dataset['diff'].apply(lambda x: 0 if x < 0 else 1)
#dataset['output'] = dataset['diff']


dataset['dma_1'] = dataset['close'].rolling(window=5, min_periods=1).mean() - dataset['close']
dataset['dma_2'] = dataset['close'].rolling(window=10, min_periods=1).mean() - dataset['close']
dataset['dma_3'] = dataset['close'].rolling(window=20, min_periods=1).mean() - dataset['close']

#
# for i in range(0, 10):
#     name = 'prev_oi_{}'.format(i)
#     dataset[name] = dataset['diff_oi'].shift(i)
#     name = 'prev_close_{}'.format(i)
#     dataset[name] = dataset['diff_close'].shift(i)
#     name = 'prev_nifty_{}'.format(i)
#     dataset[name] = dataset['diff_nifty'].shift(i)
#     name = 'prev_put_oi_{}'.format(i)
#     dataset[name] = dataset['diff_put_oi'].shift(i)
#     name = 'prev_call_oi_{}'.format(i)
#     dataset[name] = dataset['diff_call_oi'].shift(i)
#     name = 'prev_call_strike_signal_{}'.format(i)
#     dataset[name] = dataset['direct_signal'].shift(i)


def add_time_series(df, feature, look_back):
    cols = list()
    for i in range(1, look_back+1):
        col = feature + "_T-{}".format(i)
        df[col] = df[feature].shift(i)
        cols.append(col)
    return df, cols

print("Dataset: ")
dataset.info()
dataset.describe()
print(dataset[:10])

dataset, close_look_back_cols = add_time_series(dataset, "close", 100)
dataset['next_close'] = dataset['close'].shift(-1)
# dataset['output'] = dataset['next_close'] - dataset['close']
dataset['output'] = dataset['next_close']

# dataset = dataset[10:-1]
dataset = dataset.dropna()

print("Dataset: ")
dataset.info()
dataset.describe()
print(dataset[:10])

#X = dataset.iloc[:, 42:].values
#y = dataset.iloc[:, 41].values

#X = dataset.loc[:, dataset.columns != "output"].values
features = list()
fundamental_features = ['open', 'close', 'high', 'low', 'last', 'volume', 'turnover']
derived_features = ["dma_1", "dma_2", "dma_3",
                    "diff_nifty",
                    "macd_signal_diff_0", "macd_signal_diff_1",
                    "oi",
                    "put_turn", "call_turn"]

features.extend(fundamental_features)
# features.extend(close_look_back_cols)
features.extend(derived_features)

X = dataset[features]
y = dataset["output"]

print(X.shape)
print(y.shape)

# LSTM classifier
from sklearn.model_selection import train_test_split
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.layers.recurrent import LSTM
from sklearn.preprocessing import StandardScaler

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

train_accuracy_list = list()
test_accuracy_list = list()

iteration = 1
batch_size = 30
nb_epoch = 1000

validation_split = 0.20
dropout_fraction1 = 0.30
dropout_fraction2 = 0.30

cells_LSTM = 500
cells_hidden_layer1 = 1000

activation_hidden_layer1 = "relu"
activation_output_layer = 'linear'
# loss_function = "mean_absolute_percentage_error"
loss_function = "mse"

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

train_closeT = X_train["close"].values
test_closeT = X_test["close"].values

sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

X_train = np.reshape(X_train, (X_train.shape[0], 1, X_train.shape[1]))
X_test = np.reshape(X_test, (X_test.shape[0], 1, X_test.shape[1]))

fig = plt.figure(figsize=(12,12))
# ax1 = fig.add_subplot(2, 1, 1)
plots = 3

for i in range(iteration):

    # LSTM modelling
    model = Sequential()
    model.add(LSTM(cells_LSTM, input_shape=(1, X.shape[1])))
    model.add(Dropout(dropout_fraction1))

    model.add(Dense(cells_hidden_layer1, activation=activation_hidden_layer1))
    model.add(Dense(1, activation=activation_output_layer))

    model.compile(loss=loss_function, optimizer='rmsprop')

    cls_return = model.fit(X_train, y_train,
                           batch_size=batch_size,
                           epochs=nb_epoch,
                           validation_split=validation_split,
                           verbose=2)

    history = cls_return.history

    ax1 = fig.add_subplot(plots, 1, 1)
    ax1.plot(history['loss'], label='Training Loss', color='blue')
    ax1.plot(history['val_loss'], label='Validation Loss', color='black')

    # Model Evaluation
    y_pred_test = model.predict(X_test)
    y_pred_train = model.predict(X_train)
    score = model.evaluate(X_test, y_test, batch_size=batch_size)
    print(score)

    print("train_closeT: ", train_closeT.shape, type(train_closeT), train_closeT[:5])
    print("y_train.values: ", y_train.values.shape, type(y_train.values), y_train.values[:5])

    print("test_closeT: ", test_closeT.shape, type(test_closeT), test_closeT[:5])
    print("y_test.values: ", y_test.values.shape, type(y_test.values), y_test.values[:5])

    print("y_pred_train.values: ", y_pred_train.shape, type(y_pred_train), y_pred_train[:5])
    print("y_pred_test.values: ", y_pred_test.shape, type(y_pred_test), y_pred_test[:5])
    y_train_diff = y_train.values - y_pred_train
    y_test_diff = y_test.values - y_pred_test

    print(type(y_train_diff), y_train_diff)
    print(type(y_test_diff), y_test_diff)

    # Graph: prediction vs actual
    ax2 = fig.add_subplot(plots, 1, 2)
    ax2.scatter(y_train, y_pred_train, s=7, color='red')
    ax2.plot([y_train.min(), y_train.max()], [y_train.min(), y_train.max()], 'k--', label="Training", color='blue')

    ax3 = fig.add_subplot(plots, 1, 3)
    ax3.scatter(y_test, y_pred_test, s=7, color='red')
    ax3.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', label="Testing", color='blue')
    plt.show()

    movement_train_actual = y_train.values - train_closeT
    movement_test_actual = y_test.values - test_closeT

    # print(type(movement_train_actual), movement_train_actual)

    movement_train_predicted = np.ravel(y_pred_train) - train_closeT
    movement_test_predicted = np.ravel(y_pred_test) - test_closeT

    # print(y_pred_train.shape, train_closeT.shape, movement_train_predicted.shape)

    train_actual_vs_predicted_df = pd.DataFrame()
    train_actual_vs_predicted_df["movement_train_actual"] = movement_train_actual
    train_actual_vs_predicted_df["movement_train_predicted"] = movement_train_predicted
    train_actual_vs_predicted_df.sort_values(by="movement_train_actual", inplace=True)

    fig2 = plt.figure(figsize=(12, 12))

    ax1 = fig2.add_subplot(2, 1, 1)
    ax1.scatter(train_actual_vs_predicted_df["movement_train_actual"],
                train_actual_vs_predicted_df["movement_train_predicted"],
                s=7, color='red')
    ax1.scatter(train_actual_vs_predicted_df["movement_train_actual"],
                train_actual_vs_predicted_df["movement_train_actual"],
                s=7, color='blue')

    test_actual_vs_predicted_df = pd.DataFrame()
    test_actual_vs_predicted_df["movement_test_actual"] = movement_test_actual
    test_actual_vs_predicted_df["movement_test_predicted"] = movement_test_predicted
    test_actual_vs_predicted_df.sort_values(by="movement_test_actual", inplace=True)

    ax2 = fig2.add_subplot(2, 1, 2)
    ax2.scatter(test_actual_vs_predicted_df["movement_test_actual"],
                test_actual_vs_predicted_df["movement_test_predicted"],
                s=7, color='red')
    ax2.scatter(test_actual_vs_predicted_df["movement_test_actual"],
                test_actual_vs_predicted_df["movement_test_actual"],
                s=7, color='blue')

    # plt.scatter(movement_test_actual, movement_test_predicted, s=7, color='red')
    # plt.plot([movement_test_actual.min(), movement_test_actual.max()],
    #          [movement_test_actual.min(), movement_test_actual.max()],
    #          'k--', label="Testing", color='blue')

    train_evaluation_df = pd.DataFrame()
    test_evaluation_df = pd.DataFrame()

    movement_direction = np.vectorize(lambda x: 0 if x < 0 else 1)

    train_evaluation_df["y_train_movement"] = movement_direction(movement_train_actual)
    test_evaluation_df["y_test_movement"] = movement_direction(movement_test_actual)

    train_evaluation_df["y_pred_train_movement"] = movement_direction(movement_train_predicted)
    test_evaluation_df["y_pred_test_movement"] = movement_direction(movement_test_predicted)

    train_evaluation_df["train_actual_vs_pred"] = train_evaluation_df["y_train_movement"] == train_evaluation_df["y_pred_train_movement"]
    test_evaluation_df["test_actual_vs_pred"] = test_evaluation_df["y_test_movement"] == test_evaluation_df["y_pred_test_movement"]

    print(train_evaluation_df["train_actual_vs_pred"].value_counts())
    print(test_evaluation_df["test_actual_vs_pred"].value_counts())
    train_accuracy = train_evaluation_df["train_actual_vs_pred"].value_counts()[1] / train_evaluation_df.shape[0]
    test_accuracy = test_evaluation_df["test_actual_vs_pred"].value_counts()[1] / test_evaluation_df.shape[0]

    print("Train vs Test: ", train_accuracy, test_accuracy)
    train_accuracy_list.append(train_accuracy)
    test_accuracy_list.append(test_accuracy)

train_accuracy_avg = sum(train_accuracy_list) / len(train_accuracy_list)
test_accuracy_avg = sum(test_accuracy_list) / len(test_accuracy_list)
print("Avg Train vs Avg Test Accuracy: ", train_accuracy_avg, test_accuracy_avg)

# print("***Training***")
# # print(sum(cm_values_train)/len(cm_values_train))
# for cutoff, acc_list in cm_dict_train.items():
#     cm_dict_train[cutoff] = sum(acc_list) / len(acc_list)
# print(cm_dict_train)
#
# print("***Testing***")
# print(sum(cm_values_test)/len(cm_values_test))
# for cutoff, acc_list in cm_dict_test.items():
#     cm_dict_test[cutoff] = sum(acc_list) / len(acc_list)
# print(cm_dict_test)
#
# # Darshan Do Bhagwan
# sns.set()
# ax2 = fig.add_subplot(2, 1, 2)
# ax2.plot(np.linspace(.30, .70, 50), cm_dict_train.values(), c='blue')
# ax2.plot(np.linspace(.30, .70, 50), cm_dict_test.values(), c='red')
print("Execution Time(min): ", float((time.time() - start_time))/60)
plt.show()
