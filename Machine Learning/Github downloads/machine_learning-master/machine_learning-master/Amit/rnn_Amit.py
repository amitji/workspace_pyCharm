# Recurrent Neural Network

# Part 1 - Data Preprocessing

# Importing the libraries
# Importing the Keras libraries and packages
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras import optimizers
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
import math
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import quandl 
import time
import datetime
import os


def getAnyDataFromQuandl(connection_string, ticker, start_date):
        
    Authkey = '5_pRK9pKefuvZzHe-MkSy'
    mydata = quandl.get(connection_string, authtoken=Authkey, start_date=start_date, sort_order="asc")
    df = pd.DataFrame(mydata)
    filename = ticker+'.csv'
    df.to_csv(filename)

#    df.columns = ['open', 'high','low','last','close','volume','turnover']
    print ('Got data from Quandl - ', ticker)
#    df = df.drop(['open','low','high','last', 'turnover','volume'],1)
    return df
    

def buildModelAndExecute(row,final_results):
    ticker = row['ticker']
    neurons = row['neurons']
    epochs = row['epochs']
    batch_size = row['batch_size']
    lstm_layers = row['lstm_layers']
    quandl_string = row['quandl_string']
    day = datetime.datetime.now().strftime("%y-%m-%d-%H-%M")
    final_results = final_results.append({'Date':day,'ticker':ticker,'lstm_layers':lstm_layers, 'neurons':neurons,
                                      'epochs':epochs, 'batch_size':batch_size, 'ratio':'', 'percentage':''}, ignore_index=True)

    ticker_file =  ticker+'.csv'   
    file_exists = os.path.isfile(ticker_file)
    if file_exists:
        applDf = pd.read_csv(ticker_file)
    else:
        applDf = getAnyDataFromQuandl(quandl_string, ticker, '2013-01-01')
    
        
    # Importing the training set
    dataset_train = applDf[:-20]
    training_set = dataset_train.iloc[:, 1:2].values

    # Getting the real stock price of 2017
    dataset_test = applDf[-20:]
    real_stock_price = dataset_test.iloc[:, 1:2].values

    sc = MinMaxScaler(feature_range = (0, 1))
    training_set_scaled = sc.fit_transform(training_set)
    train_count = training_set_scaled.shape[0]
    # Creating a data structure with 60 timesteps and 1 output
    X_train = []
    y_train = []
    for i in range(60, train_count):
        X_train.append(training_set_scaled[i-60:i, 0])
        y_train.append(training_set_scaled[i, 0])
    X_train, y_train = np.array(X_train), np.array(y_train)
    
    # Reshaping
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    
    
    
    # Part 2 - Building the RNN
    
    
    
    # Initialising the RNN
    regressor = Sequential()
    
    
    # Adding the first LSTM layer and some Dropout regularisation
    regressor.add(LSTM(units = neurons, return_sequences = True, input_shape = (X_train.shape[1], 1)))
    regressor.add(Dropout(0.2))
    
    for i in range(0,lstm_layers-2):  # 2 are added anyways beginning and end..
        
        regressor.add(LSTM(units = neurons, return_sequences = True))
        regressor.add(Dropout(0.2))
    
    # Adding a fourth LSTM layer and some Dropout regularisation
    regressor.add(LSTM(units = neurons))
    regressor.add(Dropout(0.2))
    
    # Adding the output layer
    regressor.add(Dense(units = 1))
    
    # Compiling the RNN
    regressor.compile(optimizer = 'adam', loss = 'mean_squared_error')
    #sgd = optimizers.SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
    #regressor.compile(optimizer = 'sgd', loss = 'mean_squared_error')
    
    # Fitting the RNN to the Training set
    regressor.fit(X_train, y_train, epochs = epochs, batch_size = batch_size)
    
    
    
    # Part 3 - Making the predictions and visualising the results
    
    
    # Getting the predicted stock price of 2017
    dataset_total = pd.concat((dataset_train['Open'], dataset_test['Open']), axis = 0)
    inputs = dataset_total[len(dataset_total) - len(dataset_test) - 60:].values
    inputs = inputs.reshape(-1,1)
    inputs = sc.transform(inputs)
    X_test = []
    test_count = inputs.shape[0]
    
    for i in range(60, test_count):
        X_test.append(inputs[i-60:i, 0])
    X_test = np.array(X_test)
    X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))
    predicted_stock_price = regressor.predict(X_test)
    predicted_stock_price = sc.inverse_transform(predicted_stock_price)
    
    # Visualising the results
    plt.plot(real_stock_price, color = 'red', label = 'Real Stock Price')
    plt.plot(predicted_stock_price, color = 'blue', label = 'Predicted Stock Price')
    plt.title('Stock Price Prediction')
    plt.xlabel('Time')
    plt.ylabel('Stock Price')
    plt.legend()
    plt.show()
    
    
    rmse = math.sqrt(mean_squared_error(real_stock_price, predicted_stock_price))
    print(rmse)
    
    dfReal = pd.DataFrame(real_stock_price, columns=['rclose'])
    dfPred = pd.DataFrame(predicted_stock_price, columns=['pclose'])
    resultDf = compareMoveDirection(dfReal, dfPred)
    
    final_results['ratio'] = str(resultDf['result'].sum())+' | '+str(resultDf.shape[0])
    percentage = (resultDf['result'].sum() * 100)/resultDf.shape[0]
    final_results['percentage'] = '{0:.2f}'.format(percentage)
    print("same direction/total - ", resultDf['result'].sum()," / " ,resultDf.shape[0], ", perct = ", percentage)

    final_results = final_results[['Date','ticker','lstm_layers', 'neurons','epochs', 'batch_size', 'ratio', 'percentage']]
    print('final_results - ',final_results)

    return final_results

def compareMoveDirection(real_stock_price, predicted_stock_price):
    real_stock_price['rnext_day_close'] = real_stock_price['rclose'].shift(-1)
    predicted_stock_price['pnext_day_close'] = predicted_stock_price['pclose'].shift(-1)
    real_stock_price['rdiff'] = real_stock_price['rnext_day_close']-real_stock_price['rclose']
    #next day pridction should be substrated from actual close , not the predicted close
    #predicted_stock_price['pdiff'] = predicted_stock_price['next_day_close']-predicted_stock_price['close']
    predicted_stock_price['pdiff'] = predicted_stock_price['pnext_day_close']-real_stock_price['rclose']
    
#    print(real_stock_price)
#    print(predicted_stock_price)
    
    real_stock_price['rDirection'] = real_stock_price['rdiff'].apply(lambda x: -1 if x <0 else 1)
    predicted_stock_price['pDirection'] = predicted_stock_price['pdiff'].apply(lambda x: -1 if x <0 else 1)
    resultDf = pd.DataFrame()
    frames = [real_stock_price,predicted_stock_price]
#    resultDf = pd.concat(real_stock_price,predicted_stock_price,[real_stock_price['rDirection'], predicted_stock_price['pDirection'], real_stock_price['rdiff'],predicted_stock_price['pdiff'] ],axis=1, keys=['rDirection', 'pDirection', 'rdiff', 'pdiff']) 
    resultDf = pd.concat(frames, axis=1) 
    resultDf['result'] = resultDf.apply(lambda x: 0 if x['rDirection'] != x['pDirection'] else 1, axis=1)
    print(resultDf)
    return resultDf
    
np.set_printoptions(suppress=True)    

start_time = time.time()

final_results = pd.DataFrame()
config = pd.read_csv("lstm_config.csv")
#pick only 1's
config = config.loc[config['pick'] == 1]
 
for index, row in config.iterrows():
     
    final_results = buildModelAndExecute(row, final_results)

    filename = 'Final_Results.csv'
    file_exists = os.path.isfile(filename)
    if file_exists:
        final_results.to_csv(filename, mode='a', index=False, header=False)
    else:
        final_results.to_csv(filename, mode='a', index=False, header=True)
    
print("--- %s seconds ---" % (time.time() - start_time))
print("--- Minutes ---", float(time.time() - start_time)/60)

#diff = real_stock_price - predicted_stock_price
#diffArray = np.column_stack((real_stock_price , predicted_stock_price, diff,resultDf))
#print("\n diffArray  - \n", diffArray)
#np.savetxt('diffArray.csv',diffArray,delimiter=',', fmt='%1.2f')

