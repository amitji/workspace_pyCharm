
from nsetools import Nse


nse = Nse()
all_stock_codes = nse.get_stock_codes()

print all_stock_codes