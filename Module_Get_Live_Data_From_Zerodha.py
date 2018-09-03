import time as t
import json
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import ModuleAmitException
import zerodha_const as zc


class Module_Get_Live_Data_From_Zerodha:
    # https://kite.trade/connect/login?api_key=l5r6aemba2mjr14s&v=3
#    api_key = 'l5r6aemba2mjr14s'
#    access_token = 'lKNL7kKjGBOHkXQxCXJxZtX9XMD2Ll7p'
#    secret_key = 'h1xwmdz5u2m8otsazidtts2uxkjbqzcq'
#    request_token = 'DodCuxo7u2n2oDUFFkZGcRqx6oosUEfW'
    
    def __init__(self):
        self.kite = KiteConnect(zc.api_key)
#        self.kite.generate_session(zc.request_token, zc.secret_key)
        self.kite.set_access_token(zc.access_token)

    def getKiteObject(self):
        return self.kite
    
    def getAllQuotesFromZerodha(self, stock_names):
        
        content = []
        for row in stock_names:
            try:
                fullid = row['fullid']
                nseid = row['nseid']
                if nseid == 'NIFTY':
                    nseid = 'NIFTY 50'
                    fullid = 'NSE:NIFTY 50'
                
                quote = self.kite.quote(fullid)
                t.sleep(1)
                if quote:
                    
    #                print(fullid, " -- ", quote)
    #                s1 = json.dumps(quote)
                    s1=json.dumps(quote, indent=4, sort_keys=True, default=str)
                    data = json.loads(s1)
                    lp = data[fullid]['last_price']
                    try:
                        volume = data[fullid]['volume']
                    except Exception as e1:
                        print ("\n******No Volume  for fullid -", fullid)
    #                    print (str(e1))
                        ModuleAmitException.printInfo()
                        volume = ''
    #                    pass    
                    change = data[fullid]['net_change']
                    prev_close = data[fullid]['ohlc']['close']
                    change_percent = round((change*100)/prev_close, 2)
    #                print("lp -- ", lp)
    #                print("volume  -- ", volume)
        #            quote = self.kite.ltp(fullid)
                    content2 = {}            
                    content2['fullid'] = fullid
                    content2['nseid'] = nseid
                    content2['l'] = '{}'.format(lp).replace(",", "");
                    content2['c'] = '{0:.2f}'.format(change).replace(",", "");
                    content2['cp'] = '{}'.format(change_percent).replace(",", "");
                    content2['pcls'] = '{0:.2f}'.format(prev_close);
                    content2['volume'] = '{}'.format(volume);    
                    print(content2)
                    
                    content.append(content2);
                else:
                    print("\n*** NO qoute for - ", fullid, "\n")
            except Exception as e1:
                print ("\n******getAllQuotesFromZerodha() exception in  getting quote from Zerodha for fullid - \n ", fullid)
                print (str(e1))
                ModuleAmitException.printInfo()
#                print('\n**** Sleeping for a minute since Zerodha API had an exception...\n')
#                t.sleep(60)
                pass           

            
        return content

