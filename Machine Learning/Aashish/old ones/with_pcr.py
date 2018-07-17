import pandas as pd
call_data = pd.read_csv('call_data.csv')
put_data = pd.read_csv('put_data.csv')
call_data_needed = call_data[['Date', 'No. of contracts', 'Turnover in Lacs', 'Underlying Value']]
put_data_needed = put_data[['Date', 'No. of contracts', 'Turnover in Lacs', 'Underlying Value']]
call_data_grouped = call_data_needed.groupby(['Date']).agg({'No. of contracts': sum, 'Turnover in Lacs': sum}).reset_index()
put_data_grouped = put_data_needed.groupby(['Date']).agg({'No. of contracts': sum, 'Turnover in Lacs': sum, 'Underlying Value': max}).reset_index()
data = call_data_grouped.merge(put_data_grouped, on='Date')
data.rename(columns={'No. of contracts_x': 'call', 'No. of contracts_y': 'put', 'Turnover in Lacs_x': 'call_turn', 'Turnover in Lacs_y': 'put_turn', 'Underlying Value': 'close'}, inplace=True)
data['Date'] = pd.to_datetime(data['Date'])
dataset = data.sort_values(by=['Date'])
dataset['pcr'] = dataset['put']/dataset['call']
dataset['pcr_turn'] = dataset['put_turn']/dataset['call_turn']

dataset['next_close'] = dataset['close'].shift(-1)
dataset['diff'] = dataset['next_close'] - dataset['close']

def get_output(row):
    if row['diff']/row['close'] < -0.01:
        return 'Sell'
    elif row['diff']/row['close'] > 0.01:
        return 'Buy'
    else:
        return 'Hold'

dataset['output'] = dataset.apply(get_output, axis=1)

dataset = dataset[:-1]

dataset['prev_close'] = dataset['close'].shift(1)
dataset['prev_diff'] = dataset['close'] - dataset['prev_close']

#for i in range(0, 30):
#    name = 'prev_diff_{}'.format(i)
#    dataset[name] = dataset['prev_diff'].shift(i)
#
#dataset = dataset[30:]


#############

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

dataset = dataset[16:]

dataset['rsi_prediction'] = dataset.apply(potential_buy_sell_call_rsi, axis=1)
dataset['macd_prediction'] = dataset.apply(potential_buy_sell_call_macd, axis=1)


#############


X = dataset.iloc[:, [26, 27, 6, 7]].values
y = dataset.iloc[:, [10]].values

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder_X_1 = LabelEncoder()
X[:, 0] = labelencoder_X_1.fit_transform(X[:, 0])
labelencoder_X_2 = LabelEncoder()
X[:, 1] = labelencoder_X_2.fit_transform(X[:, 1])
onehotencoder = OneHotEncoder(categorical_features = [0, 1])
X = onehotencoder.fit_transform(X).toarray()
X = X[:, [1, 2, 3, 4, 6, 7, 8, 9, 10, 11]]

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
labelencoder_y_1 = LabelEncoder()
y[:,0] = labelencoder_y_1.fit_transform(y[:,0])
onehotencoder = OneHotEncoder(categorical_features = [0])
y = onehotencoder.fit_transform(y).toarray()
y = y[:, [1, 2]]


# Splitting the dataset into the Training set and Test set

#from sklearn.preprocessing import LabelEncoder, OneHotEncoder
#labelencoder_X_1 = LabelEncoder()
#X[:, 0] = labelencoder_X_1.fit_transform(X[:, 0])
#labelencoder_X_2 = LabelEncoder()
#X[:, 1] = labelencoder_X_2.fit_transform(X[:, 1])
#onehotencoder = OneHotEncoder(categorical_features = [0, 1])
#X = onehotencoder.fit_transform(X).toarray()
#X = X[:, 1:]
from sklearn.model_selection import train_test_split, cross_val_score
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

from sklearn.ensemble import RandomForestClassifier

classifier = RandomForestClassifier(n_estimators=200, random_state=0)
classifier.fit(X_train, y_train)

# Part 3 - Making the predictions and evaluating the model

# Predicting the Test set results
y_pred = classifier.predict(X_test)

# Making the Confusion Matrix
count = 0
count_match = 0
for i, val in enumerate(y_pred):
    if not val[0] and not y_test[i][0]:
        count += 1
        if y_test[i][0] == val[0] and y_test[i][1] == val[1]:
            count_match +=1
            
print ('count_match - ',count_match)            