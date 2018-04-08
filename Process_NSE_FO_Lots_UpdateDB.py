

import csv
import DBManager
import datetime
import requests





class Process_NSE_FO_Lots_UpdateDB:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

    def csv_reader(self):
        url = 'https://www.nseindia.com/content/fo/fo_mktlots.csv'
        r = requests.get(url)
        text = r.iter_lines()
        reader = csv.DictReader(text, delimiter=',')

        #reader = csv.DictReader(file_obj, delimiter=',')

        nseidString= ''
        count = 0
        records = []
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for line in reader:
            nseidString+=  "'%s'," % line["Symbol"]
            count +=1
            nseid = line["Symbol"]
            name = line["Company"]
            industry = line["Industry"]
            purpose = line["Purpose"]
            result_date = line["BoardMeetingDate"]
            result_date = datetime.datetime.strptime(result_date, '%d-%b-%Y').strftime('%Y-%m-%d %H:%M:%S')


            records.append((nseid,name,result_date,industry,purpose,now))

        #update the result_calendar table
        insert_sql = (
            "REPLACE INTO result_calendar  (  nseid, name, result_date, Industry, purpose, last_modified )" \
                                          "VALUES (%s, %s, %s, %s,%s, %s ) ")
        self.cur.executemany(insert_sql, records)
        self.con.commit()

        print ("Total records in CSV files Inserted/replaced into DB - ", count)
        #print nseidString




# ----------------------------------------------------------------------
thisObj = Process_NSE_FO_Lots_UpdateDB()
thisObj.csv_reader()


