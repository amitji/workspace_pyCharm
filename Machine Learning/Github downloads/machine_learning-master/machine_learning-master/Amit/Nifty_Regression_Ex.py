import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import time


from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn import preprocessing, cross_validation, svm, model_selection

import StockPredictionHelper
import StockAlgosHelper
from sklearn.neural_network import MLPRegressor

class Nifty_Regression_Ex:

    def __init__(self):
        self.helper = StockPredictionHelper.StockPredictionHelper()
        self.algohelper = StockAlgosHelper.StockAlgosHelper()
        
    def addOIAndFOMoreFeatures(self,df):
        
        days = 5
        df = self.helper.addPrevDaysAsFeature(df,'call_contracts',days)
        df = self.helper.addPrevDaysAsFeature(df,'put_contracts',days)
#        df = self.helper.addPrevDaysAsFeature(df,'call_open_int',days)
#        df = self.helper.addPrevDaysAsFeature(df,'put_open_int',days)
        df = df.drop(['call_open_int','put_open_int'],1)
        days = 5
        df = self.helper.addChangeFromPreviousDay(df,'call_contracts',days)
        df = self.helper.addChangeFromPreviousDay(df,'put_contracts',days)
#        df = self.helper.addChangeFromPreviousDay(df,'call_open_int',days)
#        df = self.helper.addChangeFromPreviousDay(df,'put_open_int',days)
        
        return df

    def addAllFeaturesOtherThanOI(self,df, flag):
        
        df = self.helper.addNiftyCloseAsFeature(df, flag)
        df['nifty_close'] = df['nifty_close'].fillna((df['nifty_close'].mean()))

#        df = df.fillna( df.mean()) 
        
        df = self.helper.moving_average(df)
        
#        df = self.helper.MACD(df)
#        df = self.helper.calculateRSI(df) 
        
        df = self.helper.addMACDAsFeature(df)       

        df = self.helper.addPrevDaysAsFeature(df,'close',60)
        
        return df        
        
        
    def backTesting(self, ticker, real_time_flag, isOI, isOtheData):
        print("\n\n\n\n\n***********  New Run of Nifty_Regression_Ex **********************\n\n")
        pd.set_option('display.expand_frame_repr', False)
#        np.set_printoptions(formatter={'float_kind':'{:f}'.format})
        np.set_printoptions(suppress=True)
        filename = 'datasets\\'+ticker+'_Final.csv'
        if real_time_flag == 1:
            df = self.helper.getData(ticker, real_time_flag)
    #        df = self.helper.getDataFromNSE(ticker, flag)
            
            df = df.fillna(df.mean())
            # Add another column next_day_close exct duplicate of close. Later we shift to make prev close.
            # First shift 'close' by -1 because today's close depends on all the feature values of yesterday. To predict today's close
            # we need the open, high, lo, volumne of yesterday because we dont have these values yet for today ! 
            df['next_day_close'] = df['close'].shift(-1)
            
            #add change & change % from previous day
            df = self.helper.addChangeFromPreviousDay(df,'close',1)
            #Use OI data or not
            if isOI ==1:           
                #old fn for total OI (options+FO together)
        #        df = self.helper.addOpenInterestData(df, ticker)
                #New OI function
                df = self.helper.addOptionsANDFoData(df, ticker)
                #this adds a NAN values to inital records...
                df = self.addOIAndFOMoreFeatures(df)
            
            if isOtheData ==1: 
                df = self.addAllFeaturesOtherThanOI(df, real_time_flag)
            
            df = df.dropna(axis=0, how='any')
            
            df.to_csv(filename)
            print('\n\n Final df columns - \n', df.columns)
        else:    
            try:
                df = pd.read_csv(filename)
                df.set_index('Date', inplace=True)
            except Exception as e:
                print('Backtesting File based fun, error is  - ', e)
                print("Now trying real_time...")
                self.backTesting(ticker, 1,  isOI, isOtheData)
                
            
        Xdf_Tomorrow = df.tail(1)  
        Xdf_Tomorrow = Xdf_Tomorrow.drop(['next_day_close'],1)
        X_Tomorrow = np.array(Xdf_Tomorrow)
        X = np.array(df.drop(['next_day_close'],1))
        X = preprocessing.scale(X)
#        X = preprocessing.normalize(X)
        
        y = np.array(df['next_day_close'])
#        y = np.array(df['close'])
#        print ("Y values - ", y[-5:])

#        print("Dataset lenghts - ",(len(X), len(y)))


        #X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.3)
        #train, validate, test = np.split(df.sample(frac=1), [int(.6*len(df)), int(.8*len(df))])
        X_train, X_rest, y_train, y_rest = model_selection.train_test_split(X, y, test_size=0.4, random_state=42)
        X_test, X_Validation, y_test, y_Validation = model_selection.train_test_split(X_rest, y_rest, test_size=0.5, random_state=42)
        
#        print("Dataset lenghts Train set- ",(len(X_train), len(y_train)))
#        print("Dataset lenghts Test set- ",(len(X_test), len(y_test)))
#        print("Dataset lenghts validation set- ",(len(X_Validation), len(y_Validation)))
        
        #result_df = pd.DataFrame()
#        result_df.columns = [['Actual Close', 'Linear Reg', 'SVM', 'Decision Tree']]
#        self.result_df.loc[0, 'Actual Close'] = X_Validation[0]

#        df.loc[df.shape[0]] = ['d', 3] 
        
#        self.callRandomForest(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#        self.algohelper.callDecisionTreeRegressor(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#        self.algohelper.callGradientBoosting(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#        
#        self.algohelper.callLinearRegression(X_train, X_test, y_train, y_test,X_Validation, y_Validation, X_Tomorrow )
##        self.algohelper.callLogisticRegression(X_train, X_test, y_train, y_test,X_Validation , y_Validation , X_Tomorrow)
#        self.algohelper.callSVM(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#        self.algohelper.callXGBClassifier(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#        self.algohelper.callKNN(X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
#    
        #Neural Network
        df = self.callNeuralNetwork(ticker,X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow)
        return df
    
    def callNeuralNetwork(self, ticker,X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow ):
        #Random forest
#        clf = MLPRegressor(alpha=0.001, solver='lbfgs', hidden_layer_sizes = (100,), max_iter = 100000, 
#                 activation = 'logistic', learning_rate = 'adaptive')
        result = pd.DataFrame()
        
        #For testing        
#        activationL = ['logistic'] #activationL = ['logistic','tanh','relu']
#        solverL = ['lbfgs'] #solverL = ['lbfgs', 'sgd', 'adam']
#        learning_rateL=['adaptive'] #learning_rateL=['constant', 'invscaling', 'adaptive'] 
#        hidden_layer_sizesL=[10] #hidden_layer_sizesL=[10,50,100]

        activationL = ['logistic','tanh','relu']
        solverL = ['lbfgs', 'sgd', 'adam']
        learning_rateL=['constant', 'invscaling', 'adaptive'] 
        hidden_layer_sizesL=[10,50,100]
        
        for activationV in activationL:
            for solverV in solverL:
                for learning_rateV in learning_rateL:
                    for hidden_layer_sizesV in hidden_layer_sizesL:
                    
                        try:
                            clf = MLPRegressor(alpha=.001, solver=solverV, hidden_layer_sizes = (hidden_layer_sizesV,), max_iter = 500000, 
                                     activation = activationV, verbose = False, learning_rate = learning_rateV)
                    
                    
                            clf.fit(X_train, y_train)
                            
                            y_predicted = clf.predict(X_Validation)
                            accuracy = clf.score(X_Validation, y_Validation)
                            diff = y_Validation - y_predicted
                            diffArray = np.column_stack((y_Validation , y_predicted, diff))
#                            print("\n NeuralNetwork diffArray  - \n", diffArray)
#                            print ("\n****** NeuralNetwork Accuracy  - ",accuracy)
                    
                            mse = mean_squared_error(y_Validation, y_predicted)
#                           print ("Mean Squred Error", mse)
#                            variance = np.var(y_predicted)         
#                           print ("Varinace - ", variance)
                            
                            y_Tomorrow = clf.predict(X_Tomorrow)
#                            print ("X_Tomorrow close - ", X_Tomorrow[0][0])
#                            print ("y_Tomorrow - ", y_Tomorrow)
                            diff = y_Tomorrow - X_Tomorrow[0][0]
                            result = result.append({'ticker':ticker,'hidden_layer_sizes':hidden_layer_sizesV, 'activation':activationV, 'solver':solverV, 'learning_rate':learning_rateV, 'accuracy':accuracy,'Y_actual':X_Tomorrow[0][0], 'Y_predicted':y_Tomorrow[0], 'Diff':diff[0], 'mse':mse}, ignore_index=True)
            
                            print('Result data Frame - \n', result)
                        except Exception as e:
                            print('Error in MLPRegressor for ticker   - ', ticker)
                            print('error is - ',e)
                            continue

        return result
        
if __name__ == "__main__":
    start_time = time.time()
    thisObj = Nifty_Regression_Ex()
    #MARUTI, AUROPHARMA, 8KMILES, FEDERALBNK, NCC, GMRINFRA, HDFCBANK,ICICIBANK
    # INFY,ITC,ONGC, 
#    tickerL = ['ICICIBANK','MARUTI','AUROPHARMA', 'FEDERALBNK','HDFCBANK' ]
    tickerL = ['ICICIBANK','MARUTI']
    
    real_time_flag = 1
    results = pd.DataFrame()
    for ticker in  tickerL:
        result = thisObj.backTesting(ticker, real_time_flag, 1, 1)  # include OI data or not
        results = results.append(result)
        results = results[['ticker','Y_actual', 'Y_predicted','Diff','mse','accuracy','hidden_layer_sizes','activation', 'solver', 'learning_rate']]                        
        filename = 'datasets\\'+ticker+'_Regression_Results.csv'
        results.to_csv(filename)
        
    results = results[['ticker','Y_actual', 'Y_predicted','Diff','mse','accuracy','hidden_layer_sizes','activation', 'solver', 'learning_rate']]                        
    filename = 'datasets\\Combined_Regression_Results.csv'
    results.to_csv(filename)
        
#    thisObj.backTesting(ticker, 'csv', 1, 1)  # include OI data or not
    
#    thisObj.predictNextDayPrice()

    print ("Done !!")
    print("--- %s seconds ---" % (time.time() - start_time))
    print("--- %s Minutes ---" % (time.time() - start_time)/60)