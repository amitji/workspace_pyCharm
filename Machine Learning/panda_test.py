import os
import pandas as pd
import matplotlib.pyplot as plt


def test_run():
    start_date = '2017-08-23'
    end_date = '2017-09-21'
    dates = pd.date_range(start_date, end_date)
    #print dates
    df1 = pd.DataFrame(index=dates)

    #print df1
    dfNifty = pd.read_csv("data\NIFTY.csv", index_col="Date", parse_dates=True, usecols=['Date', 'Close Price'], na_values=['nan'])
    dfNifty = dfNifty.rename(columns={'Close Price': 'NIFTY'})
    #join removes rows other than dfNifty and copies dfNifty col to df
    df1 =df1.join(dfNifty, how='inner')
    #read in more stcoks
    symbols=['SUZLON', 'GMRINF', 'DIVISLAB']
    for symbol in symbols:
        df_temp = pd.read_csv("data\{}.csv".format(symbol), index_col="Date", parse_dates=True, usecols=['Date', 'Close Price'],
                    na_values=['nan'])
        df_temp = df_temp.rename(columns={'Close Price': symbol})
        df1=df1.join(df_temp)

    #df1.dropna()
    print df1
    #slice by row range (dates) using DataFrame.ix[] selector
    #print df1.ix['2017-09-01': '2017-09-11']
    #print df1[['SUZLON', 'DIVISLAB']]  # list of labels selects multiple columns
    #Slice by row and column
    #print df1.ix['2017-09-01': '2017-09-11', ['SUZLON', 'DIVISLAB']]


    #df1=df1/df1.iloc[0]
    df1 = df1/df1.ix[0,:]
    plot_data(df1)

def plot_data(df,title="Stock Prices"):
    ax = df.plot(title=title, fontsize=8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")

    plt.show()  # must be called

if __name__ == "__main__":
    test_run()