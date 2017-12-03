# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from sklearn import linear_model

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list the files in the input directory
#from subprocess import check_output
#print(check_output(["ls", "../input"]).decode("utf8"))

df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\Titanic\\train.csv")
print df.head()
#df.drop(label)
print (df['survived'].mean())

X = np.array(df.drop(['Survived'],1))
Y = np.array(df['Survived'])
print X
print Y
clf = linear_model.LogisticRegression(C=1.0)
clf.fit(X,Y)

df = pd.read_csv("D:\\workspace_pyCharm\\Machine Learning\\datasets\\Titanic\\test.csv")
print df
X_test = np.array(df)
Y_test = np.array()
print X_test
print Y_test

accuracy = clf.score(X_test, Y_test)
print accuracy


# Any results you write to the current directory are saved as output.
print ("amit 111")