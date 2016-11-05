#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import Module_Scrapper_Screener_India_Stocks

class Scrapper_Screener_India_Stocks:
    def __init__(self):
        self.module_Scrapper_Screener_India_Stocks = Module_Scrapper_Screener_India_Stocks.Module_Scrapper_Screener_India_Stocks()


thisObj = Scrapper_Screener_India_Stocks()
stock_names= thisObj.getStockList()
thisObj.module_Scrapper_Screener_India_Stocks.updateAll(stock_names)