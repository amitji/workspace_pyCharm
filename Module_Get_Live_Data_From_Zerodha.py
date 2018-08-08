import time as t
import json
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect


class Module_Get_Live_Data_From_Zerodha:
    # https://kite.trade/connect/login?api_key=l5r6aemba2mjr14s&v=3
    api_key = 'l5r6aemba2mjr14s'
    access_token = 'lshcXxuYqCZPNywDsQ7bgKecf0dS8DQG'
    secret_key = 'h1xwmdz5u2m8otsazidtts2uxkjbqzcq'
    request_token = 'DodCuxo7u2n2oDUFFkZGcRqx6oosUEfW'
    
    def __init__(self):
        self.kite = KiteConnect(self.api_key)
#        self.kite.generate_session(self.request_token, self.secret_key)
        self.kite.set_access_token(self.access_token)

    def getKiteObject(self):
        return self.kite
    
    def getAllQuotesFromZerodha(self, stock_names):
        
        content = []
        for row in stock_names:
            fullid = row['fullid']
            quote = self.kite.quote(fullid)
            t.sleep(1)
            if quote:
                
                print(fullid, " -- ", quote)
#                s1 = json.dumps(quote)
                s1=json.dumps(quote, indent=4, sort_keys=True, default=str)
                data = json.loads(s1)
                lp = data[fullid]['last_price']
                volume = data[fullid]['volume']
                change = data[fullid]['net_change']
                prev_close = data[fullid]['ohlc']['close']
                change_percent = round((change*100)/prev_close, 2)
#                print("lp -- ", lp)
#                print("volume  -- ", volume)
    #            quote = self.kite.ltp(fullid)
                content2 = {}            
                content2['fullid'] = fullid
                content2['l'] = '{}'.format(lp).replace(",", "");
                content2['c'] = '{}'.format(change).replace(",", "");
                content2['cp'] = '{}'.format(change_percent).replace(",", "");
                content2['pcls'] = '{}'.format(prev_close);
                content2['volume'] = '{}'.format(volume);    
                print(content2)
                
                content.append(content2);
            
        return content
            
# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
#kws.connect()