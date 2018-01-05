# -*- coding: utf-8 -*-

import pandas as pd
#import pandas.io.data as web
from pandas_datareader import data as web

stocks = ['FB']
def get_stock(stock, start, end):
     return web.get_data_yahoo(stock, start, end)['Adj Close']
px = pd.DataFrame({n: get_stock(n, '1/1/2016', '12/31/2016') for n in stocks})
print (px)