from googlefinance import getQuotes
import json

print json.dumps(getQuotes('NSE:HDIL'))