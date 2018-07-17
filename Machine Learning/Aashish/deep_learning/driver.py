from data_preprocessing import Data
from lstm_model_builder import LSTMModelBuilder
from test_results import TestLSTMResults
from argparse import ArgumentParser
from datetime import datetime
import pandas as pd
import time
import os


def write_results_in_file(final_results, file_name):
    # Store the results in file
    file_exists = os.path.isfile(file_name)
    if file_exists:
        final_results.to_csv(file_name, mode='a', index=False, header=False)
    else:
        final_results.to_csv(file_name, mode='a', index=False, header=True)


def parse_arguments():
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-f', "--config_file_path", dest='config_file_path',  help="pass the config file path",
                            default='config_file.csv')
    args = arg_parser.parse_args()

    return args


def main():
    args = parse_arguments()
    config_file_path = args.config_file_path
    config = pd.read_csv(config_file_path)
    config = config.loc[config['pick'] == 1]  # pick only 1's
    final_result_file = 'Final_Results_{}.csv'.format(datetime.now().strftime("%Y%m%d_%H%M%S"))

    for index, row in config.iterrows():
        print("Processing row: {}".format(index))
        start_time = time.time()

        # Get row by row data from config file
        ticker = row['ticker']
        neurons = row['neurons']
        epochs = row['epochs']
        batch_size = row['batch_size']
        lstm_layers = row['lstm_layers']
        quandl_string = row['quandl_string']
        fields = row['fields']
        train_test_split_ratio = row['train_test_split_ratio']
        look_back_days = row['look_back_days']
        start_date = row['start_date']

        # Dataframe to store the results for every config row
        day = datetime.now().strftime("%Y-%m-%d %H:%M")
        final_results = pd.DataFrame(row).transpose()
        final_results['Date'] = day

        # Get  data for the specified ticker
        data_object = Data(ticker, quandl_string, fields, start_date)
        data_df = data_object.get_data()

        # Get train, test scaled data
        X_train, y_train, X_test, y_test = data_object.pre_process_data(data_df, look_back_days, train_test_split_ratio)

        # Build the model
        model_builder_object = LSTMModelBuilder(X_train, y_train, neurons, epochs, batch_size, lstm_layers)
        model = model_builder_object.build_and_execute_model()
        y_pred = model_builder_object.get_predicted_results(model, X_test)

        # Test the predicted results
        test_object = TestLSTMResults(y_test, y_pred)
        cm, true_percentage = test_object.compare_movement_direction()
        rmse, rmse_avg = test_object.get_rmse()

        final_results['true_percentage'] = true_percentage
        final_results['one_one'] = cm[1][1]
        final_results['zero_zero'] = cm[0][0]
        final_results['zero_one'] = cm[0][1]
        final_results['one_zero'] = cm[1][0]
        final_results['rmse'] = rmse
        final_results['rmse_avg'] = rmse_avg
        final_results['time_taken'] = time.time() - start_time

        # Write the results to file
        write_results_in_file(final_results, final_result_file)

main()
