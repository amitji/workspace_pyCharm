from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from model_bulider import ModelBuilder


class LSTMModelBuilder(ModelBuilder):
    def __init__(self, X_train, y_train, neurons, epochs, batch_size, lstm_layers):
        super().__init__(X_train, y_train)
        self.__neurons = neurons
        self.__epochs = epochs
        self.__batch_size = batch_size
        self.__lstm_layers = lstm_layers

    def build_and_execute_model(self, *args, **kwargs):
        # Initialising the RNN
        regressor = Sequential()

        # Adding the first LSTM layer and some Dropout regularisation
        regressor.add(LSTM(units=self.__neurons, return_sequences=True,
                           input_shape=(self._X_train.shape[1], self._X_train.shape[2])))
        regressor.add(Dropout(0.2))

        for i in range(0, self.__lstm_layers - 2):  # 2 are added anyways beginning and end..
            regressor.add(LSTM(units=self.__neurons, return_sequences=True))
            regressor.add(Dropout(0.2))

        # Adding LSTM layer and some Dropout regularisation
        regressor.add(LSTM(units=self.__neurons))
        regressor.add(Dropout(0.2))

        # Adding the output layer
        regressor.add(Dense(units=1))

        # Compiling the RNN
        regressor.compile(optimizer='adam', loss='mean_squared_error')

        # Fitting the RNN to the Training set
        regressor.fit(self._X_train, self._y_train, epochs=self.__epochs, batch_size=self.__batch_size)

        return regressor
