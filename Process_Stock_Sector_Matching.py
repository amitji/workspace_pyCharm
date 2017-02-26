#Amit Purpose - Run this program to update the last_price, 52 week high , 52 week low etc for all the stcoks in
# fa_financial_ratio_us_stocks table


import DBManager

class Process_Stock_Sector_Matching:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()


    def getStockList(self):
        #select_sql = "select  name, industry_vertical from stocksdb.stock_names sn where exchange='NSE' and name like '%Munjal Auto%' "
        select_sql = "select  name, industry_vertical,industry_sub_vertical  from stocksdb.stock_names sn where exchange='NSE' order by name "
        self.cur.execute(select_sql)
        rows = self.cur.fetchall()
        sn_data = {}
        for row in rows:
            # print row[0], row[1]
            temp_list = [row[0],row[1],row[2]]
            sn_data[row[0]] = temp_list
        #print sn_data

        select_sql = "select  name, industry,industry_sub from stocksdb.stock_sector sn order by name"
        self.cur.execute(select_sql)
        rows = self.cur.fetchall()
        loopup_data = {}
        for row in rows:
            # print row[0], row[1]
            temp_list = [row[0],row[1],row[2]]
            loopup_data[row[0]] = temp_list
        #print loopup_data

        #full_data = list()
        for k, v in sn_data.iteritems():
            try:
                #print "\n", k, v
                name_first_word = k.split(None, 1)[0]
                print "name_first_word - ",name_first_word
                for k2, v2 in loopup_data.iteritems():
                    #print "k2, v2", k2, v2
                    if k in k2:
                        print "\n\nExact match !!"
                        sn_data[k][1] = v2[1]
                        sn_data[k][2] = v2[2]
                        break
                    else:
                        if name_first_word not in k2:
                            continue
                        else:
                            try:
                                #print k, " -- ", k2
                                #print v, " -- ",v
                                #sn_data[k] = v+","+v2
                                sn_data[k][1] = sn_data[k][1]+","+v2[1]
                                sn_data[k][2] = sn_data[k][2]+","+v2[2]
                                #print "updated v (industry)", sn_data[k]
                            except Exception, e1:
                                print str(e1)
                                print "\n******Amit - Exception in looping Stock names & Sectors K, V- ", k, v
                                pass
                #remove ',' from begining
                sn_data[k][1] = sn_data[k][1].lstrip(',')
                sn_data[k][2] = sn_data[k][2].lstrip(',')

            except Exception, e2:
                print str(e2)
                print "\n******Amit - Exception in looping Stock names & Sectors K, V- ", k, v
                pass



            #print "\n\n****************final k,v " , k,v
        #print "Final sn_data - \n\n",sn_data
        return sn_data

    def updateSectorsForStock(self, sn_data):


            for k, v in sn_data.iteritems():
                try:
                    #data = (fullid, percentage_rating, latest_quarter, revT, profitT, opmT, pmT, ebitT, roeT, icT, deT, total, now, now)
                    if v :
                        updateSql = "update stock_names set industry_vertical = '%s', industry_sub_vertical='%s' where name = '%s' " % (v[1],v[2],k)

                        self.cur.execute(updateSql)
                        print "\n*** Updated stock, industry - ", k, v
                        self.con.commit()
                    else:
                        print "\n*** Industry blank for  - ", k

                except Exception, e1:
                    print str(e1)
                    print "\n******Amit - Exception in UPDATING Stock names & Sectors K, V- ", k, v
                    pass

thisObj = Process_Stock_Sector_Matching()
sn_data= thisObj.getStockList()
thisObj.updateSectorsForStock(sn_data)


