# Recurrent Neural Network



# Part 1 - Data Preprocessing

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import quandl

ticker = 'INFY'

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

dataset['next_close'] = dataset['close'].shift(-1)






####################


###################



dataset_train = dataset[:400]
dataset_test = dataset[400:]


# Importing the training set
training_set = dataset_train.iloc[:, [2, 3, 6, 7, 9, 10, 11]].values

# Feature Scaling
from sklearn.preprocessing import MinMaxScaler
sc = MinMaxScaler(feature_range = (0, 1))
sc_close = MinMaxScaler(feature_range = (0, 1))
training_set_scaled = sc.fit_transform(training_set)

# Creating a data structure with 60 timesteps and 1 output
X_train = []
y_train = []
for i in range(60, 400):
    X_train.append(training_set_scaled[i-60:i, :])
    y_train.append(training_set_scaled[i, 6])
X_train, y_train = np.array(X_train), np.array(y_train)

# Reshaping
X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 7))



# Part 2 - Building the RNN

# Importing the Keras libraries and packages
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout

# Initialising the RNN
regressor = Sequential()

# Adding the first LSTM layer and some Dropout regularisation
regressor.add(LSTM(units = 50, return_sequences = True, input_shape = (X_train.shape[1], 7)))
regressor.add(Dropout(0.2))

# Adding a second LSTM layer and some Dropout regularisation
regressor.add(LSTM(units = 50, return_sequences = True))
regressor.add(Dropout(0.2))

# Adding a third LSTM layer and some Dropout regularisation
regressor.add(LSTM(units = 50, return_sequences = True))
regressor.add(Dropout(0.2))

# Adding a fourth LSTM layer and some Dropout regularisation
regressor.add(LSTM(units = 50))
regressor.add(Dropout(0.2))

# Adding the output layer
regressor.add(Dense(units = 1))

# Compiling the RNN
regressor.compile(optimizer = 'adam', loss = 'mean_squared_error')

# Fitting the RNN to the Training set
regressor.fit(X_train, y_train, epochs = 100, batch_size = 32)



# Part 3 - Making the predictions and visualising the results

# Getting the real stock price of 2017
real_stock_price = dataset_test.iloc[:, 11:12].values

# Getting the predicted stock price of 2017
dataset_mid_test = dataset[len(dataset) - len(dataset_test) - 60:]
inputs = dataset_mid_test.iloc[:, [2, 3, 6, 7, 9, 10, 11]].values

inputs = inputs.reshape(-1,7)
inputs = sc.transform(inputs)
X_test = []
for i in range(60, 137):
    X_test.append(inputs[i-60:i, :])
X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 7))
predicted_stock_price = regressor.predict(X_test)
predicted_stock_price = sc.inverse_transform(predicted_stock_price)

# Visualising the results
plt.plot(real_stock_price, color = 'red', label = 'Real Google Stock Price')
plt.plot(predicted_stock_price, color = 'blue', label = 'Predicted Google Stock Price')
plt.title('Google Stock Price Prediction')
plt.xlabel('Time')
plt.ylabel('Google Stock Price')
plt.legend()
plt.show()


import math
from sklearn.metrics import mean_squared_error
rmse = math.sqrt(mean_squared_error(real_stock_price, predicted_stock_price))
