
#from yahoo_finance import Share
#from googlefinance import getQuotes
#from googlefinance.client import get_price_data, get_prices_data, get_prices_time_data


import json

'''
#yahoo = Share('INFY')
#print yahoo.get_open()
print json.dumps(getQuotes('AAPL'), indent=2)
'''

import json

import requests

#rsp = requests.get('https://finance.google.com/finance?q=NSE:SUZLON&output=json')
rsp = requests.get('https://finance.google.com/finance?q=TYO:7203&output=json')

if rsp.status_code in (200,):

    # This magic here is to cut out various leading characters from the JSON
    # response, as well as trailing stuff (a terminating ']\n' sequence), and then
    # we decode the escape sequences in the response
    # This then allows you to load the resulting string
    # with the JSON module.
    fin_data = json.loads(rsp.content[6:-2].decode('unicode_escape'))

    # print out some quote data
    print('Last Trade Price: {}'.format(fin_data['l']))
    print('Change: {}'.format(fin_data['c']))
    print('Change %: {}'.format(fin_data['cp']))
    #print('Prev Close: {}'.format(fin_data['pc']))
    print('Opening Price: {}'.format(fin_data['op']))
    print('Price/Earnings Ratio: {}'.format(fin_data['pe']))
    print('52-week high: {}'.format(fin_data['hi52']))
    print('52-week low: {}'.format(fin_data['lo52']))