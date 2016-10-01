
import quandl
import pandas
import operator

def __init__(self):
    print "Calling GetNSELiveData constructor"



def getNSELiveData( nseid):
    Authkey = '5_pRK9pKefuvZzHe-MkS'
    nse_dataset = "NSE" + "/" + nseid
    mydata = quandl.get(nse_dataset, authtoken=Authkey, rows=250, sort_order="desc")

    # print mydata
    df = pandas.DataFrame(mydata)
    dd = df.to_dict(orient='dict')
    ddd = dd['Close']

    # print df
    # print dd
    # print ddd
    # print max(ddd, key=ddd.get)
    maxKey = max(ddd.iteritems(), key=operator.itemgetter(1))[0]
    maxVal = ddd.get(maxKey)
    print "High52 - ", maxVal

    # print min(ddd, key=ddd.get)
    minKey = min(ddd.iteritems(), key=operator.itemgetter(1))[0]
    minVal = ddd.get(minKey)
    print "Low52 - ", minVal

    last_price = df['Close'][0]

    print "Last Price - ", last_price

    nsedata = dict()
    nsedata["high52"] = maxVal
    nsedata["low52"] = minVal
    nsedata["last_price"] = last_price
    return nsedata
