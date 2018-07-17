

import csv
import DBManager
import datetime
import requests
import EmailUtil
import urllib.request
from io import TextIOWrapper




class NSE_Result_Calendar_Update_Process:

    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

    def csv_reader(self):
        url = 'https://www.nseindia.com/corporates/datafiles/BM_All_ForthcomingResults.csv'
        r = requests.get(url)
        decoded_content = r.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        reader = list(cr)
    
#        text = r.iter_lines()
#        reader = csv.DictReader(text, delimiter=',')

#        ftpstream = urllib.request.urlopen(url)
#        reader = csv.reader(ftpstream)  # with the appropriate encoding 
#        data = [row for row in csvfile]

        nseidString= ''
        count = 0
        records = []
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #skil line one with titles
        for line in reader[1:]:
            count +=1
            #['Symbol', 'Company', 'Industry', 'Purpose', 'BoardMeetingDate']    
            nseidString+=  " , "+line[0]
            
            nseid = line[0]
            name = line[1]
            industry = line[2]
            purpose = line[3]
            result_date = line[4]
            result_date = datetime.datetime.strptime(result_date, '%d-%b-%Y').strftime('%Y-%m-%d %H:%M:%S')


            records.append((nseid,name,result_date,industry,purpose,now))

        
        
        self.updateAmitPortfolioTable(records)
        #update the result_calendar table
        insert_sql = (
            "REPLACE INTO result_calendar  (  nseid, name, result_date, Industry, purpose, last_modified )" \
                                          "VALUES (%s, %s, %s, %s,%s, %s ) ")
        self.cur.executemany(insert_sql, records)
        self.con.commit()

        #Amit  =  update amit_portfolio table with resul dates

        
        
        print( "Total records in CSV files Inserted/replaced into DB - ", count)
        EmailUtil.send_email_as_text("Process_NSE_Result_Calendar_Download_n_UpdateDB", nseidString, "")

    def updateAmitPortfolioTable(self, records):
        
        sql = "SELECT ap.nseid, rc.result_date FROM stocksdb.amit_portfolio ap, result_calendar rc where ap.nseid = rc.nseid order by ap.nseid, rc.result_date "
        #sql = "SELECT distinct fullid FROM " + table_name + " where fullid like '%-%'  order by fullid "

        self.cur.execute(sql)
        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            #dd["fullid"] = row[0]
            dd["nseid"] = row[0]
            #dd["result_date"] = row[1].strftime('%d-%m-%Y')
            dd["result_date"] = row[1].strftime('%Y-%m-%d %H:%M:%S')
            #dd["data_type"] = row[1]
            data.append(dd)
        # print data
        

        for rec in data[:]:
            print (rec)
            nseid = rec['nseid']
            rdate = rec['result_date']
            update_sql = "update amit_portfolio set result_date = '%s' where nseid = '%s' " % (rdate,nseid);
            self.cur.execute(update_sql)
            self.con.commit()
        


# ----------------------------------------------------------------------
thisObj = NSE_Result_Calendar_Update_Process()
thisObj.csv_reader()



