import numpy as np
import pandas as pd
from math import sqrt
import matplotlib.pyplot as plt
from matplotlib import style
from collections import Counter

style.use('fivethirtyeight')

dataset = {'k': [[1,2], [2,3],[3,1]], 'r':[[6,5],[7,7],[8,6]]}
new_features = [5,7]

df = pd.read_csv('.\\datasets\\breast-cancer-wisconsin.data')
df.replace('?', -99999, inplace=True)
df.drop(['id'],1,inplace=True )

print df
full_data = df.values.tolist()
print full_data
full_data = df.astype(int).values.tolist()
print full_data

#plt.scatter()
