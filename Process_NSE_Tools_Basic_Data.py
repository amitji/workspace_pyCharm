
from nsetools import Nse


nse = Nse()
all_stock_codes = nse.get_stock_codes(cached=False, as_json=False)

print (all_stock_codes)