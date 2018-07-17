import pandas as pd
prices = pd.read_csv("data/prices-split-adjusted.csv")
prices.info()
print(prices[0:10])

#import seaborn as sns
#sns.set()

import matplotlib.pyplot as plt

import numpy as np
exclude_cols = ['date', 'symbol']
numeric_cols = prices.columns.tolist()
numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
print(numeric_cols)
#print(prices[numeric_cols][0:10])
#heat_map = sns.heatmap(prices[numeric_cols][0:50])
#plt.show()

print(prices[ : 3])
prices["close_nextday"] = prices.close.shift(-1)
print(prices[ : 3])


prices_corr = prices.corr()
print(prices_corr)
#prices_corr_heat_map = sns.heatmap(prices_corr)
#plt.show()

prices = prices.dropna(axis=0)
print(prices.info())
print(prices[ : 3])

print(prices.symbol.unique().size)
print(prices.date.unique().size)

#prices = prices[numeric_cols]
prices_corr = prices.corr()
#prices_corr_heat_map = sns.heatmap(prices_corr)
#plt.show()

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

prices_WLTW = prices[prices['symbol'] == 'GOOG']
mse_values_test = list()
mse_values_train = list()
variance_values = list()

"""
plt.scatter(X_test['open'], y_test, color = 'red')
plt.scatter(X_test['open'], y_pred, color = 'blue')
plt.show()
"""

features = ['low', 'high', 'open', 'close', 'volume']
for index in range(1, 5):
    X_train, X_test, y_train, y_test = train_test_split(prices_WLTW[features[0:index]],
                                                        prices_WLTW['close_nextday'],
                                                        test_size=.33)

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred_train = lr.predict(X_train)
    y_pred_test = lr.predict(X_test)

    mse_values_train.append(mean_squared_error(y_train, y_pred_train))
    mse_values_test.append(mean_squared_error(y_test, y_pred_test))

print(mse_values_train)
print(mse_values_test)

# Visualising the Test set results
plt.plot(range(1, 5), mse_values_train, label='training error')
plt.plot(range(1, 5), mse_values_test, label='test error')
plt.show()



