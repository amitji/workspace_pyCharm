import logging
from kiteconnect import KiteTicker
import time as t
import zerodha_const as zc

logging.basicConfig(level=logging.DEBUG)

# Initialise
#api_key = 'l5r6aemba2mjr14s'
#access_token = 'lKNL7kKjGBOHkXQxCXJxZtX9XMD2Ll7p'
#public_token ='14hgHq6qD4jFj12pinniHTZHlvg4gc2Q'
#user_id = 'UE6253'

kws = KiteTicker(zc.api_key, zc.access_token)
#tokens = ['NSE:NCC', 'NSE:INFY']
tokens = [408065, 884737]

def on_ticks(ws, ticks):
    # Callback to receive ticks.
    logging.debug("Ticks: {}".format(ticks))
#    print("sleeping for 10 sec after getting ticks...")
#    t.sleep(10)
    

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(tokens)

    # Set RELIANCE to tick in `full` mode. MODE_LTP, MODE_QUOTE, or MODE_FULL.
    ws.set_mode(ws.MODE_FULL, tokens)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

def on_error(ws, code, reason):
    logging.error("closed connection on error: {} {}".format(code, reason))


def on_noreconnect(ws):
    logging.error("Reconnecting the websocket failed")


def on_reconnect(ws, attempt_count):
    logging.debug("Reconnecting the websocket: {}".format(attempt_count))


def on_order_update(ws, data):
    print("order update: ", data)
    
# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error
kws.on_noreconnect = on_noreconnect
kws.on_reconnect = on_reconnect
kws.on_order_update = on_order_update

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
#kws.connect()

print('calling KWS.connect')
kws.connect(threaded=True)
print('finished kws connect process')
