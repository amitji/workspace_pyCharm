from abc import abstractmethod


class ModelBuilder:
    def __init__(self, X_train, y_train):
        self._X_train = X_train
        self._y_train = y_train

    @abstractmethod
    def build_and_execute_model(self, *args, **kwargs):
        pass

    @staticmethod
    def get_predicted_results(model, X_test):
        return model.predict(X_test)
