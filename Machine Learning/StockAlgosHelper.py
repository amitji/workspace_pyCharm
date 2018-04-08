
from sklearn.neighbors import KNeighborsClassifier,KNeighborsRegressor
from xgboost import XGBClassifier,XGBRegressor
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn import preprocessing, cross_validation, svm, model_selection
from sklearn.linear_model import LinearRegression, LogisticRegression
import matplotlib.pyplot as plt
from matplotlib import style
from sklearn import tree
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
import numpy as np # linear algebra
import pandas as pd 

class StockAlgosHelper:

    def callRandomForest(self,ticker, X_train, X_test, y_train, y_test, X_Tomorrow ):
        #Random forest
        clf = RandomForestRegressor(min_samples_leaf=1, max_features=0.5, n_estimators=20, random_state=42, oob_score=True)
        clf.fit(X_train, y_train)
        return self.processRestOfAlgo('RandomForest',clf,ticker, X_train, X_test, y_train, y_test, X_Tomorrow)
        
    def callDecisionTreeRegressor(self, ticker, X_train, X_test, y_train, y_test, X_Tomorrow):
        #Decision Tree
        clf = tree.DecisionTreeRegressor()
        clf.fit(X_train, y_train)
        return self.processRestOfAlgo('DecisionTree',clf,ticker, X_train, X_test, y_train, y_test, X_Tomorrow)
        
        
    def callGradientBoosting(self,ticker, X_train, X_test, y_train, y_test, X_Tomorrow):
        #GradientBoostingClassifier
        clf= GradientBoostingRegressor(n_estimators=100, learning_rate=1.0, max_depth=1, random_state=0)
        clf.fit(X_train, y_train)
        return self.processRestOfAlgo('GradientBoosting',clf,ticker, X_train, X_test, y_train, y_test, X_Tomorrow)
    
    def processRestOfAlgo(self, algo, clf,ticker, X_train, X_test, y_train, y_test, X_Tomorrow):
        Y_Predicted = clf.predict(X_test)
        accuracy = clf.score(X_test, y_test)
        diff = y_test - Y_Predicted
        diffArray = np.column_stack((y_test , Y_Predicted, diff))
        print("\n diffArray  - \n", diffArray)
        print (algo, "******  Accuracy  - ",accuracy)

        mse = mean_squared_error(y_test, Y_Predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(Y_Predicted)         
        print ("Varinace - ", variance)
        
        
        Y_Predicted = clf.predict(X_Tomorrow)
        Y_Predicted = Y_Predicted[0]
        Y_Actual = X_Tomorrow[0][0]
        diff = Y_Predicted - Y_Actual
        diffPerct = (diff/Y_Actual) * 100
        prev_day_close = X_Tomorrow[0][1]  # prev Close position in Array

        print ("prev_day_close - ", prev_day_close)
        print ("Y_Actual - ", Y_Actual)
        print ("Y_Predicted - ", Y_Predicted)
        print ("Diff & Diff % - ", diff, ' , ', diffPerct, '%' )    

        
        if (Y_Actual - prev_day_close) > 0:
            Actual_Direction =  1
        else:
            Actual_Direction = -1
        if (Y_Predicted - prev_day_close) > 0:
            Predicted_Direction =  1
        else:
            Predicted_Direction = -1
            
        if Predicted_Direction == Actual_Direction:
            Directional_Outcome = True
        else:
            Directional_Outcome = False
        
        
        data = [{'Algo':algo,'ticker':ticker,'accuracy':accuracy,'Prev_Day_Close':prev_day_close , \
                        'Y_Actual':Y_Actual, 'Y_Predicted':Y_Predicted, 'Diff':diff,'DiffPerct':diffPerct,'Directional_Outcome':Directional_Outcome, 'mse':mse}]
        
        df = pd.DataFrame(data)
        return df 

    def callLinearRegression(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow):
        # Linear regression
        clf = LinearRegression(n_jobs=-1)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        
        y_predicted = clf.predict(X_Validation)
#        accuracy = clf.score(X_Validation, y_Validation )
#        print ("\n\n****** LinearRegression Accuracy  - ",accuracy)
        
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
#        print("\n diffArray  - \n", diffArray)
        print ("\n****** LinearRegression Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)

        y_Tomorrow = clf.predict(X_Tomorrow)
        print ("X_Tomorrow close - ", X_Tomorrow[0][0])
        print ("y_Tomorrow - ", y_Tomorrow)
        

    def callKNN(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow):
         #KNN
        clf = KNeighborsRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
#        print("\n diffArray  - \n", diffArray)
        print ("\n****** KNeighborsRegressor Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
        y_Tomorrow = clf.predict(X_Tomorrow)
        print ("X_Tomorrow close - ", X_Tomorrow[0][0])
        print ("y_Tomorrow - ", y_Tomorrow)
        
    def callXGBClassifier(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow):
        #XGBClassifier
        clf = XGBRegressor()
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
#        print("\n diffArray  - \n", diffArray)
        print ("\n****** XGBRegressor Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        
        
        y_Tomorrow = clf.predict(X_Tomorrow)
        print ("X_Tomorrow close - ", X_Tomorrow[0][0])
        print ("y_Tomorrow - ", y_Tomorrow)

    def callLogisticRegression(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation , X_Tomorrow):
        # Linear regression
        clf = LogisticRegression(penalty='l2',C=.01)
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
#        print("\n diffArray  - \n", diffArray)
        print ("\n****** LogisticRegression Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        

        y_Tomorrow = clf.predict(X_Tomorrow)
        print ("X_Tomorrow close - ", X_Tomorrow[0][0])
        print ("y_Tomorrow - ", y_Tomorrow)
        
    def callSVM(self, X_train, X_test, y_train, y_test,X_Validation , y_Validation, X_Tomorrow ):
        # SVM        
        clf = svm.SVR(kernel='poly')
        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)
        y_predicted = clf.predict(X_Validation)
        diff = y_Validation - y_predicted
        diffArray = np.column_stack((y_Validation , y_predicted, diff))
#        print("\n diffArray  - \n", diffArray)
        print ("\n****** SVM Accuracy  - ",accuracy)

        mse = mean_squared_error(y_Validation, y_predicted)
        print ("Mean Squred Error", mse)
        variance = np.var(y_predicted)         
        print ("Varinace - ", variance)        

        y_Tomorrow = clf.predict(X_Tomorrow)
        print ("X_Tomorrow close - ", X_Tomorrow[0][0])
        print ("y_Tomorrow - ", y_Tomorrow)
        