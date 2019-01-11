import os
import zipfile
import requests
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import DBManager


download_path = os.path.join(str(Path(__file__).resolve().parent), "downloads")
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
        Path(download_path).mkdir(parents=True, exist_ok=True)
        # Make sure we also have other directories
        for current_exchange in supported_exchanges:
            Path(os.path.join(download_path, current_exchange)).mkdir(parents=True, exist_ok=True)
    
    def yesterday(self):
        """
        formats date in british format
        """
        yesterday = datetime.now() - timedelta(days=1)
        return str(yesterday.date().strftime("%d/%m/%Y"))
    
    def download(self,download_url, file_path):
        """
        download function is used to fetch the data
        """
        print("Downloading file at", file_path)
    
        # Don't download file if we've done that already
        if not os.path.exists(file_path):
            file_to_save = open(file_path, "wb")
            with requests.get(download_url, verify=False, stream=True) as response:
                for chunk in response.iter_content(chunk_size=1024):
                    file_to_save.write(chunk)
            print("Completed downloading file")
        else:
            print("We already have this file cached locally")
    
    def download_and_unzip(self,download_url, file_path):
        """
        download_and_unzip takes care of both downloading and uncompressing
        """
        self.download(download_url, file_path)
        with zipfile.ZipFile(file_path, "r") as compressed_file:
            compressed_file.extractall(Path(file_path).parent)
        print("Completed un-compressing")
    
    def download_nse_bhavcopy(self,for_date):
        """
        this function is used to download bhavcopy from NSE
        """
        for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
        month = for_date_parsed.strftime("%b").upper()
        year = for_date_parsed.year
        day = "%02d" % for_date_parsed.day
        url = f"https://www.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
        file_path = os.path.join(download_path, "nse", f"cm{day}{month}{year}bhav.csv.zip")
        try:
            self.download_and_unzip(url, file_path)
        except zipfile.BadZipFile:
            print(f"Skipping downloading data for {for_date}")
            return
        os.remove(file_path)
        return file_path
    
    def download_bse_bhavcopy(self,for_date):
        """
        this function is used to download bhavcopy from BSE
        """
        for_date_parsed = datetime.strptime(for_date, "%d/%m/%Y")
        month = "%02d" % for_date_parsed.month
        day = "%02d" % for_date_parsed.day
        year = for_date_parsed.strftime("%y")
        file_name = f"EQ{day}{month}{year}_CSV.ZIP"
        url = f"http://www.bseindia.com/download/BhavCopy/Equity/{file_name}"
        file_path = os.path.join(download_path, "bse", file_name)
        try:
            self.download_and_unzip(url, file_path)
        except zipfile.BadZipFile:
            print(f"Skipping downloading data for {for_date}")
        os.remove(file_path)
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
                else:
                    file_path = self.download_bse_bhavcopy(ts)
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
        prev_day_df = self.getPrevDayMarketData();
        
        df = pd.read_csv(file_path)
        df = df.rename(columns={'OPEN': 'open', 'HIGH': 'high','LOW': 'low' ,'LAST': 'last' ,'CLOSE': 'close' , \
                           'SYMBOL': 'nseid' ,'PREVCLOSE': 'prev_day_close','TOTTRDQTY': 'volume' ,\
                           'TOTTRDVAL': 'turnover', 'TIMESTAMP':'my_date'  })
        df = df.drop(['ISIN', 'SERIES', 'TOTALTRADES', 'Unnamed: 13'], axis=1 )
#        pd.to_datetime(df['my_date']).apply(lambda x: x.date())
        df['my_date'] = pd.to_datetime(df['my_date'], format='%d-%b-%Y')
        
#        df.columns = ['open', 'high', 'low','last', 'close', 'volume', 'turnover']
#        df['nseid'] = nseid
#        df['my_date'] = df.index
#        df['prev_day_close'] = df['close'].shift(-1)
        df['perct_change'] = ((df['close'] - df['prev_day_close'])* 100)/df['prev_day_close']
        df['perct_change'] = df['perct_change'].round(2)

#        for i, row in df.iterrows():
#            nseid = row['nseid'] 
#            prev_day_vol = prev_day_df.loc[prev_day_df['nseid'] == nseid , 'volume' ]
##            prev_day_vol = prev_day_df.query('nseid=='+nseid2)['volume']
#            df.at[i,'prev_day_vol'] = prev_day_vol
        
        df['prev_day_vol'] = ''
        for i, row in prev_day_df.iterrows():
            nseid = row['nseid']
            prev_day_vol = row['volume']
            
            df.loc[df['nseid'] == nseid, 'prev_day_vol'] = prev_day_vol
#            print("updated prev_vol for ", nseid)
        
        df["prev_day_vol"] = pd.to_numeric(df["prev_day_vol"])    

        #Amit - for now we dont have pre  day vol so keep it zero
#        df['prev_day_vol'] = prev_day_df['volume'] prev_day_df.loc[df['nseid'] == 3, 'A']
#        df['prev_day_vol'] = df['volume'].shift(-1)
        df['vol_chg_perct'] = ((df['volume'] - df['prev_day_vol'])* 100)/df['prev_day_vol']
        df['vol_chg_perct'] = df['vol_chg_perct'].round(2)
#        df = df.drop(['prev_day_vol'],1)
        
        #Amit - save only last record in database...
#        df_latest = df.iloc[0]   
        df_latest = df[:1]   
        df.to_sql('stock_market_data', self.engine, if_exists='append', index=False) 
        print("saveLastRecordInDB done ")
        
    
        
        
        
        
        
thisObj = Process_NSE_BhavCopy_Download_and_Update_DB()
thisObj.createDirectory()
#file_path = thisObj.execute("nse",thisObj.yesterday(),1)
file_path = thisObj.execute("nse","10/01/2019",1)
file_path = file_path[:-4] 
print(file_path)
thisObj.saveToDB(file_path)

