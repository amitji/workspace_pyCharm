import os
import requests 
import zipfile, io
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import DBManager
import EmailUtil
import time
import sys

download_path = os.path.join(str(Path(__file__).resolve().parent), "downloads")
#cron job needs complete path name...
#directory = "//home//shopbindaas//python-workspace//downloads//nse"
#for windows
directory = ".//downloads//nse"

#supported_exchanges = ["bse", "nse"]
supported_exchanges = ["nse"]

class Process_NSE_BhavCopy_Download_and_Update_DB:

    def __init__(self):
        print ("Calling parent constructor")

        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
#        self.cur = self.con.cursor()
#        self.qd_exception_list = []
#        self.fr_exception_list = []

    
    def createDirectory(self):
        # Make sure we have downloads directory
        if not os.path.exists(directory):
            os.makedirs(directory)
    
#        Path(download_path).mkdir(parents=True, exist_ok=True)
#        # Make sure we also have other directories
#        for current_exchange in supported_exchanges:
#            
#            Path(os.path.join(download_path, current_exchange)).mkdir(parents=True, exist_ok=True)
    
    def yesterday(self):
        """
        formats date in british format
        """
        yesterday = datetime.now() - timedelta(days=1)
        return str(yesterday.date().strftime("%d/%m/%Y"))
    def today(self):
        """
        formats date in british format
        """
        today = datetime.now()
        return str(today.date().strftime("%d/%m/%Y"))
    
    def download(self,download_url, file_path):
        hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
        
        r = requests.get(download_url, headers = hdr)
        with open(file_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
                
        print("Amit - file extracted")

        # with urllib.request.urlopen(download_url) as dl_file:
        #     with open(file_path, 'wb') as out_file:
        #         out_file.write(dl_file.read())

    
    # def download(self,download_url, file_path):
    #     r = requests.get(download_url)
    #     z = zipfile.ZipFile(io.BytesIO(r.content))
    #     z.extractall(directory)
    #     print("Amit - file extracted")
        
        
        
    # def download(self,download_url, file_path):
    #     """
    #     download function is used to fetch the data
    #     """
    #     print("Downloading file at", file_path)
        
    #     try:
    #         # Don't download file if we've done that already
    #         if not os.path.exists(file_path):
    #             file_to_save = open(file_path, "wb")
    #             with requests.get(download_url, verify=False, stream=True) as response:
    #                 for chunk in response.iter_content(chunk_size=1024):
    #                     file_to_save.write(chunk)
    #             print("Amit - Completed downloading file")
    #         else:
    #             print("Amit - We already have this file cached locally")
    #     except exception as e:
    #          print("Amit - Error in downloading file...", str(e))
    #          #return
    #          sys.exit(1)

    
    def download_and_unzip(self,download_url, file_path):
        """
        download_and_unzip takes care of both downloading and uncompressing
        """
        
        # self.download(download_url, file_path)
        
        try:    
            with zipfile.ZipFile(file_path, "r") as compressed_file:
    #            compressed_file.extractall(Path(file_path).parent)
                compressed_file.extractall(directory)


            # compressed_file = zipfile.ZipFile(file_path, "r")   
            # compressed_file.extractall(directory)
            # time.sleep(15)
            # compressed_file.close()
                
                
            print("Completed un-compressing")
        except zipfile.BadZipFile as e1:
             print("Amit - Error in decompressing file...", str(e1))
             #return
             sys.exit(1)
        #print("Amit sleeping for a minute, check file")
        #time.sleep(60)
    
    def download_nse_bhavcopy(self,for_date):
        """
        this function is used to download bhavcopy from NSE
        """
        for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
        month = for_date_parsed.strftime("%b").upper()
        year = for_date_parsed.year
        day = "%02d" % for_date_parsed.day
        #      https://www1.nseindia.com/content/historical/EQUITIES/2020/JUL/cm13JUL2020bhav.csv.zip
        url = "https://www1.nseindia.com/content/historical/EQUITIES/{year1}/{month1}/cm{day1}{month2}{year2}bhav.csv.zip".format(year1=year,month1=month,day1=day,month2=month,year2=year)
        print("Downloding file URL - ",url)
        file_path = os.path.join(download_path, "nse", "cm{day1}{month1}{year1}bhav.csv.zip".format(day1=day,month1=month,year1=year))
        print ("bhavcopy nse url - ",url)
        self.download_and_unzip(url, file_path)

        print("Amit - looks like download and decompression successful !")    
        #os.remove(file_path)
        return file_path
    
    
    
    def execute(self,exchange, for_date, for_past_days):
        """
        download_bhavcopy is utility that will download daily bhav copies
        from NSE and BSE
    
        Examples:
        python download_bhavcopy.py bse --for_date 06/12/2017
    
        python download_bhavcopy.py bse --for_past_days 15
        """
    #    click.echo("downloading bhavcopy {exchange}")
    
        # We need to fetch data for past X days
        if for_past_days != 1:
            for i in range(for_past_days):
                ts = datetime.now() - timedelta(days=i+1)
                ts = ts.strftime("%d/%m/%Y")
                if exchange == "nse":
                    file_path = self.download_nse_bhavcopy(ts)
#                else:
#                    file_path = self.download_bse_bhavcopy(ts)
        else:
            if exchange == "nse":
                file_path = self.download_nse_bhavcopy(for_date)
            else:
                file_path = self.download_bse_bhavcopy(for_date)
        
    
        return file_path
        
    def getPrevDayMarketData(self):
        
        select_sql = "select * FROM stock_market_data where my_date in (select max(my_date) from stock_market_data )"
        
        #testing sql
#        select_sql = "select * FROM stock_market_data where my_date in (select max(my_date) from stock_market_data ) and nseid in ('8KMILES','20MICRONS')"
        
        df = pd.read_sql(select_sql, self.con)
#        df = df.apply(lambda x: x.str.strip())
        return df  
        
    def saveToDB(self, file_path):
        #get prev day daya for calculating volume change
        print("Amit - saving data now to Database")
        prev_day_df = self.getPrevDayMarketData();
        
        df = pd.read_csv(file_path)
        #filter only series type 'EQ'
        df = df.loc[(df['SERIES'] == 'EQ') | (df['SERIES'] == 'BZ') | (df['SERIES'] == 'BE')]
        
        df = df.rename(columns={'OPEN': 'open', 'HIGH': 'high','LOW': 'low' ,'LAST': 'last' ,'CLOSE': 'close' , \
                           'SYMBOL': 'nseid' ,'PREVCLOSE': 'prev_day_close','TOTTRDQTY': 'volume' ,\
                           'TOTTRDVAL': 'turnover', 'TIMESTAMP':'my_date'  })
        df = df.drop(['ISIN', 'SERIES', 'TOTALTRADES', 'Unnamed: 13'], axis=1 )
#        pd.to_datetime(df['my_date']).apply(lambda x: x.date())
        df['my_date'] = pd.to_datetime(df['my_date'], format='%d-%b-%Y')
        
        df['perct_change'] = ((df['close'] - df['prev_day_close'])* 100)/df['prev_day_close']
        df['perct_change'] = df['perct_change'].round(2)
        df['prev_day_vol'] = ''
        
        for i, row in prev_day_df.iterrows():
            nseid = row['nseid']
            prev_day_vol = row['volume']
            df.loc[df['nseid'] == nseid, 'prev_day_vol'] = prev_day_vol
            #print("updated prev_vol for - ", nseid, "  and prev vol is - ",prev_day_vol)
        
        df["prev_day_vol"] = pd.to_numeric(df["prev_day_vol"])    
        df['vol_chg_perct'] = ((df['volume'] - df['prev_day_vol'])* 100)/df['prev_day_vol']
        df['vol_chg_perct'] = df['vol_chg_perct'].round(2)
        
        print("Stocks to be updated in stock_market_data table -  \n", df)
        #Amit - save only last record in database...
        df.to_sql('stock_market_data', self.engine, if_exists='append', index=False) 
        print("Table stock_market_data  is updated with latest data....")
        
        #update fa_financial_ratio and fa_financial_ratio_secondary for last_price etc.
        self.update_other_tables(df)
        
        return df
        
    
    def update_other_tables(self,df):
        
        
         #write df into a temp table
        df.to_sql('stock_market_data_temp_table', self.engine, if_exists='replace')
        
        # update fa_financial_ratio table
        update_sql = " update fa_financial_ratio fa inner join stock_market_data_temp_table t "
        update_sql += " on fa.nseid = t.nseid set fa.last_price = t.close, fa.last_modified = now() "         
        with self.engine.begin() as conn:
            conn.execute(update_sql)  
        print("updated last_price in fa_financial_ratio table ")  
        
        #update fa_financial_ratio_secondary table... 
        #update_sql = " update fa_financial_ratio_secondary fa inner join stock_market_data_temp_table t "
        #update_sql += " on fa.nseid = t.nseid set fa.last_price = t.close, fa.last_modified = now() "         
        #with self.engine.begin() as conn:
        #   conn.execute(update_sql)                    
        #print("updated last_price in fa_financial_ratio_secondary table ")    
        
        
        
        
        #update amit_portfolio table... 
        update_sql = " update amit_portfolio ap inner join stock_market_data_temp_table t "
        update_sql += " on ap.nseid = t.nseid set ap.last_trade_price = t.close, ap.previous_close = t.prev_day_close,  ap.last_modified = now() , "         
        update_sql += " ap.price_change = (t.close - t.prev_day_close), ap.change_perct= t.perct_change, ap.volume = t.volume "         
        
        with self.engine.begin() as conn:
            conn.execute(update_sql)                    
        print("updated amit_portfolio table ")    


        
        
thisObj = Process_NSE_BhavCopy_Download_and_Update_DB()

df = pd.DataFrame()
#### testing
#thisObj.update_other_tables(df)
#####

thisObj.createDirectory()

#file_path = thisObj.execute("nse",thisObj.yesterday(),1)
file_path = thisObj.execute("nse","05/05/2021",1)
#file_path = thisObj.execute("nse",thisObj.today(),1)

print("Amit - Looks like DB save is all good.... ")    

if file_path == None:
    print("File might not be available for this date.. may be weekend ! ")
else:
    file_path = file_path[:-4] 
    print(file_path)
    df = thisObj.saveToDB(file_path)
    df = df[['nseid','close', 'prev_day_vol']]
    print(df)

EmailUtil.send_email_with_body("Process_NSE_BhavCopy_Download_and_Update_DB",df.to_string())
#/Users/amimahes/opt/anaconda3/bin/python "/Users/amimahes/Dropbox/Amit_Work-dbox/Github Repos/workspace_pyCharm/Process_NSE_BhavCopy_Download_and_Update_DB.py"
