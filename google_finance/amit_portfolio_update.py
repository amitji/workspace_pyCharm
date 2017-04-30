import urllib2
import json
import time
import DBManager


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

    def saveIntoDB(self, records):

        print "\n*** Amit saving qoutes to database"

        # for record in records:
        #     print record
        #     print record[0], record[1]
        #     sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s where nseid=%s"
        #     self.cur.execute(sql, tuple(record))

        #use batch execute rahter above  1by 1
        sql = "update amit_portfolio set last_trade_price=%s,  price_change=%s, change_perct=%s, previous_close=%s where nseid=%s"
        self.cur.executemany(sql, records)
        self.con.commit()

if __name__ == "__main__":
    c = GoogleFinanceAPI()
    stock_names = c.getStockList()
    records = [] ## LIST OF LISTS
    minutes_count = 0  # compare with 7 Hrs run daily from 9-4 pm (7*60=420)
    print "\n*** Amit Started getting quotes"
    while minutes_count < 420:
        for row in stock_names:

            try:
                #print "callin get for  - ", row['nseid']
                quote = c.get(row['fullid'])
                #print quote
                records.append(( quote['l_fix'], quote['c_fix'],quote['cp_fix'],quote['pcls_fix'], row['nseid']) )
            except Exception, e:
                print "\n******Amit couldnt get Google quotes for  " + row['fullid']
                print str(e)
                pass

        c.saveIntoDB(records)
        minutes_count = minutes_count+1
        print "\n*** Amit Sleeping for 1 minute"
        time.sleep(60)