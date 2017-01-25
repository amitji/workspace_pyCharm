

import csv
import DBManager
import datetime
import requests





class NSE_Result_Calendar_Update_Process:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

    def csv_reader(self):
        url = 'https://www.nseindia.com/corporates/datafiles/BM_All_ForthcomingResults.csv'
        r = requests.get(url)
        text = r.iter_lines()
        reader = csv.DictReader(text, delimiter=',')

        #reader = csv.DictReader(file_obj, delimiter=',')

        nseidString= ''
        count = 0
        records = []
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for line in reader:
            #print line
            #print(line["Symbol"]),
            #print(line["BoardMeetingDate"])
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

        print "Total records in CSV files Inserted/replaced into DB - ", count
        #print nseidString




# ----------------------------------------------------------------------
thisObj = NSE_Result_Calendar_Update_Process()
thisObj.csv_reader()


