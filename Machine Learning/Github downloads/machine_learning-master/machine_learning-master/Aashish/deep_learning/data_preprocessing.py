from constants import AUTH_KEY
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import quandl
import os


class Data:
    def __init__(self, ticker, connection_string, fields, start_date):
        self.__ticker = ticker
        self.__connection_string = connection_string
        self.__fields = fields.split('|')
        self.__start_date = start_date

    def __get_data(self):
        my_data = quandl.get(self.__connection_string, authtoken=AUTH_KEY, start_date=self.__start_date, sort_order="asc")
        df = pd.DataFrame(my_data)
        df.columns = ['open', 'high', 'low', 'last', 'close', 'volume', 'turnover']

        return df

    def get_data(self):
        file_name = "{}_{}{}".format(self.__ticker, self.__start_date, '.csv')
        file_exists = os.path.isfile(file_name)
        if file_exists:
            data = pd.read_csv(file_name)
        else:
            data = self.__get_data()
            data.to_csv(file_name)

        data = data[self.__fields]  # Fetch only the columns that are needed

        return data

    @staticmethod
    def pre_process_data(data, look_back_days, train_test_split_ratio):
        number_of_columns = data.shape[1]
        data['next_close'] = data['close'].shift(-1)
        data = data[:-1]

        split_index = int(len(data) * (1 - train_test_split_ratio))
        data_train = data[:split_index]
        data_test = data[split_index:]

        # Importing the training set
        training_set = data_train.iloc[:, 0:-1].values

        # Feature Scaling
        sc_input = MinMaxScaler(feature_range=(0, 1))
        training_set_scaled = sc_input.fit_transform(training_set)

        X_train = []
        for i in range(look_back_days, split_index):
            X_train.append(training_set_scaled[i - look_back_days:i, :])

        y_train = data_train.iloc[look_back_days:, -1]
        # y_train = sc_output.fit_transform(y_train)
        X_train, y_train = np.array(X_train), np.array(y_train)

        # Reshaping
        X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], number_of_columns))

        # Getting the test data
        data_mid_test = data[len(data) - len(data_test) - look_back_days:]
        inputs = data_mid_test.iloc[:, 0:-1].values

        inputs = inputs.reshape(-1, number_of_columns)
        inputs = sc_input.transform(inputs)
        X_test = []
        for i in range(look_back_days, len(inputs)):
            X_test.append(inputs[i - look_back_days:i, :])
        X_test = np.array(X_test)
        X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], number_of_columns))

        y_test = data_test.iloc[:, -1].values

        return X_train, y_train, X_test, y_test
