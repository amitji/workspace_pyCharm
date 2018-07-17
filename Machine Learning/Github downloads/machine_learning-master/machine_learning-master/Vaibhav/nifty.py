# Modelling and metrics
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_squared_error
import numpy as np

# Plots
import matplotlib.pyplot as plt
import seaborn as sns

# Fetch market data
import GetStockMarketData as stock_data

# First glimpse at data
prices = stock_data.getData('NIFTY_50')
prices.info()
print(prices[0:10])

# Set target
target = "close_T+1"
print(prices[ : 3])
prices[target] = prices.close.shift(-1)
print(prices[ : 3])

# Filter out NaN
prices = prices.dropna(axis=0)
prices.info()

# Inspect co-relation
prices_corr = prices.corr()
print(prices_corr)
#sns.set()
prices_corr_heat_map = sns.heatmap(prices_corr)
plt.show()

# Metrics
scores_train = list()
scores_test = list()
mse_values_test = list()
mse_values_train = list()
mses_cv = list()
variance_values = list()
mse_polynomial = dict()

from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
degrees = [1, 2, 3]

fig = plt.figure()
folds_count = 5
features = ['low', 'high', 'open', 'close', 'turnover', 'volume']
for index in range(1, len(features)):
    X_train, X_test, y_train, y_test = train_test_split(prices[features[0:index]], prices[target], test_size=.33,)

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    scores_train.append(lr.score(X_train, y_train))
    scores_test.append(lr.score(X_test, y_test))
    y_pred_train = lr.predict(X_train)
    y_pred_test = lr.predict(X_test)

    mse_polynomial[index+1] = list()
    for i in range(len(degrees)):
        polynomial_features = PolynomialFeatures(degree=degrees[i], include_bias=False)
        pipeline = Pipeline([("polynomial_features", polynomial_features),
                             ("linear_regression", lr)])
        pipeline.fit(prices[features[0:index]], prices[target])

        # Evaluate the models using crossvalidation
        scores = cross_val_score(pipeline, prices[features[0:index]], prices[target],
                                 scoring="neg_mean_squared_error", cv=folds_count)
        mse_polynomial[index+1].append(np.mean(np.absolute(scores)))

    kf = KFold(folds_count, shuffle=True)
    mses = cross_val_score(lr, prices[features[0:index]], prices[target], scoring="neg_mean_squared_error", cv=kf)
    mses_cv.append(np.mean(np.absolute(mses)))

    """
    # Graph: prediction vs actual
    plt.scatter(y_test, y_pred_test, s=7, color='red')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', color='blue')
    plt.show()
    """

    mse_values_train.append(mean_squared_error(y_train, y_pred_train))
    mse_values_test.append(mean_squared_error(y_test, y_pred_test))

print(scores_train)
print(scores_test)
print(mse_values_train)
print(mse_values_test)
print(mses_cv)
print(mse_polynomial)

# Visualising the Test set results
plt.plot(range(2, 7), mse_values_train, c='blue')
plt.plot(range(2, 7), mse_values_test, c='red')
plt.plot(range(2, 7), mses_cv, c='green')
plt.show()

colors = ['b', 'g', 'r', 'c', 'y']
labels = ['feature_count: 2', 'feature_count: 3', 'feature_count: 4', 'feature_count: 5', 'feature_count: 6', ]
for i in range(2, 7):
    plt.plot(range(len(mse_polynomial[i])), mse_polynomial[i], 'k--', lw=1, c=colors[i-2], label=labels[i-2])
plt.legend(loc='best')
plt.show()
