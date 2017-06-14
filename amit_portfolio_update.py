import urllib2
import json
import DBManager
import pandas
from datetime import datetime, time
import EmailUtil
import time as t

class GoogleFinanceAPI:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.prefix = "http://finance.google.com/finance/info?client=ig&q="

    def getStockList(self):
        select_sql ="select fullid, nseid from stocksdb.amit_portfolio where display_seq is not null order by display_seq "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            data.append(dd)
        # print data
        return data

    def get(self, symbol):
        url = self.prefix + "%s" % ( symbol)
        u = urllib2.urlopen(url)
        content = u.read()

        obj = json.loads(content[3:])
        return obj[0]

    def getAllQuotes(self, stock_names):
        try:
            content = []
            content2 = []
            url = self.prefix
            for row in stock_names:
                url = url+ "%s" % ( row['fullid']) + ","

            u = urllib2.urlopen(url)

            content = u.read()
            content = content.replace('\n', '')
            content = content[2:]
            #content = content[:-1]
            #content2 = content.split("\n}\n,")
            content = json.loads(content, "ISO-8859-1")
        except Exception, e:
            print "\n******Amit exception in getAllQuotes "
            print str(e)

        return content


    def saveIntoDB(self, allQuotes):

        print "\n*** Amit saving qoutes to database"
        records = []
        fullid= ""
        for row in allQuotes:

            try:
                fullid = row['e']+":"+row['t']
                #print "fullid - ", fullid
                record = (( row['l_fix'], row['c_fix'],row['cp_fix'],row['pcls_fix'], fullid))
                print record
                records.append(record )

            except Exception, e:
                print "\n******Amit couldnt get Google quotes for  " + fullid
                print str(e)
                pass

        # for record in records:
        #     print record
        #     print record[0], record[1]
        #     sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s where nseid=%s"
        #     self.cur.execute(sql, tuple(record))

        #use batch execute rahter above  1by 1
        sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s where fullid=%s"
        self.cur.executemany(sql, records)
        self.con.commit()

    def in_between(self,now, start, end):
        if start <= end:
            return start <= now < end
        else:  # over midnight e.g., 23:30-04:15
            return start <= now or now < end

if __name__ == "__main__":
    c = GoogleFinanceAPI()
    stock_names = c.getStockList()
    records = [] ## LIST OF LISTS
    minutes_count = 0  # compare with 7 Hrs run daily from 9-4 pm (7*60=420)
    EmailUtil.send_email_as_text(" amit_portfolio_update.py job started - ", "", "")
    print "\n*** Amit Started getting quotes"

    #while minutes_count < 420:
    #Run b/w morning 9 am to 4:00 pm IST
    while (c.in_between(datetime.now().time(), time(9), time(16,00))):
        allQuotes = c.getAllQuotes(stock_names)
        c.saveIntoDB(allQuotes)
        minutes_count = minutes_count+1
        print "\n*** Amit Sleeping for 1 minute, remaining loops (420-x)- ", 420- minutes_count, " | Time - ", datetime.now()
        t.sleep(60)

    print "\n*** Amit Exiting the google quote process...TIME is  - ", datetime.now().time()