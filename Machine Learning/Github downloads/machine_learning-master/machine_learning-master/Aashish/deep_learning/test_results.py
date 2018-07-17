from sklearn.metrics import confusion_matrix
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import pandas as pd
import math

class TestLSTMResults:
    def __init__(self, real_results, predicted_results):
        self.__predicted_results = predicted_results
        self.__real_results = real_results

    def compare_movement_direction(self):
        df_real = pd.DataFrame(self.__real_results, columns=['close'])
        df_pred = pd.DataFrame(self.__predicted_results, columns=['close'])
        df_real['next_day_close'] = df_real['close'].shift(-1)
        df_pred['next_day_close'] = df_pred['close'].shift(-1)
        df_real['diff'] = df_real['next_day_close'] - df_real['close']

        # next day prediction should be subtracted from actual close , not the predicted close
        df_pred['diff'] = df_pred['next_day_close'] - df_real['close']

        df_real['direction'] = df_real['diff'].apply(lambda x: -1 if x < 0 else 1)
        df_pred['direction'] = df_pred['diff'].apply(lambda x: -1 if x < 0 else 1)
        cm = confusion_matrix(df_real['direction'], df_pred['direction'])
        true_percentage = (cm[0][0] + cm[1][1])/ (cm[0][0] + cm[1][1] + cm[0][1] + cm[1][0])
        return cm, true_percentage

    def plot_graph(self):
        plt.plot(self.__real_results, color='red', label='Real Stock Price')
        plt.plot(self.__predicted_results, color='blue', label='Predicted Stock Price')
        plt.title('Stock Price Prediction')
        plt.xlabel('Time')
        plt.ylabel('Stock Price')
        plt.legend()
        plt.show()

    def get_rmse(self):
        rmse = math.sqrt(mean_squared_error(self.__real_results, self.__predicted_results))
        rmse_avg = rmse/self.__real_results.mean()
        return rmse, rmse_avg
