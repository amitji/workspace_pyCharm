
import json
import quandl
import DBManager
import datetime
import math
import pandas
import operator
import time
#from nsetools import Nse

import NSELiveDataModule


class QuandlDataModule:
    # cur = ""


    def __init__(self):
        # print "Calling parent constructor"
        self.Authkey = '5_pRK9pKefuvZzHe-MkS'
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()
        self.qd_exception_list = []
        self.fr_exception_list = []
        #self.nse_exception_list = []
        self.updated_stock_list = []

        self.all_good_flag = True
        # quater_names = {'2016-05-16': 'QQQ', '2016-03-31': 'Q415', '2015-12-31': 'Q315', '2015-09-30': 'Q215',
        #               '2015-06-30': 'Q115', '2015-03-31': 'Q414', '2014-12-31': 'Q314'}
        self.quater_names = {'2016-06-30': 'Q116', '2016-03-31': 'Q415', '2015-12-31': 'Q315', '2015-09-30': 'Q215',
                        '2015-06-30': 'Q115', '2015-03-31': 'Q414', '2014-12-31': 'Q314'}

        # _stocks = self.getStocks()

    # def __del__(self):
    #     # self.con.close
    #     # self.cur.close()
    #     print "\n\n*****  deleting FinalRatingCalculationProcess  "

    """
    def getStocksEnabledForVendorData(self):
        self.cur.execute("SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '1' and update_now = 'y' order by nseid")
        #self.cur.execute("SELECT distinct nseid, data_type FROM stock_names where enable_for_vendor_data = '1' order by nseid")
        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            # print row[0], row[1]
            dd = dict()
            dd["nseid"] = row[0]
            dd["data_type"] = row[1]
            data.append(dd)
        # print data
        return data
    """


    def updateQuarterlyData(self, row, table_name):

        self.all_good_flag = True

        Vendor_Name = 'DEB'
        revenue_dataset_suffix = '_Q_SR'
        profit_dataset_suffix = '_Q_NP'

        profit_margin_dataset_suffix = '_Q_NETPCT'
        operating_profit_dataset_suffix = '_Q_OP'
        operating_prof_margin_dataset_suffix = '_Q_OPMPCT'
        ebidt_dataset_suffix = '_Q_EBIDT'
        ebidt_margin_dataset_suffix = '_Q_EBIDTPCT'


        nseid = row["nseid"]
        type = row["data_type"]

        fullid = "NSE:"+nseid

        print fullid
        # print str(fullid)

        nseidModified = nseid.replace("&", "")
        rev_dataset = Vendor_Name+"/"+nseidModified+revenue_dataset_suffix
        profit_dataset = Vendor_Name + "/" + nseidModified + profit_dataset_suffix

        profit_margin_dataset = Vendor_Name + "/" + nseidModified + profit_margin_dataset_suffix
        operating_profit_dataset = Vendor_Name + "/" + nseidModified + operating_profit_dataset_suffix
        operating_prof_margin_dataset = Vendor_Name + "/" + nseidModified + operating_prof_margin_dataset_suffix
        ebidt_dataset = Vendor_Name + "/" + nseidModified + ebidt_dataset_suffix
        ebidt_margin_dataset = Vendor_Name + "/" + nseidModified + ebidt_margin_dataset_suffix


        try:
            revData = quandl.get(rev_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")
            profitData = quandl.get(profit_dataset , authtoken=self.Authkey, rows=5, sort_order="desc")

            profitMarginData = quandl.get(profit_margin_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")
            operatingProfitData = quandl.get(operating_profit_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")
            operatingProfitMarginData = quandl.get(operating_prof_margin_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")
            ebidtData = quandl.get(ebidt_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")
            ebidtMarginData = quandl.get(ebidt_margin_dataset, authtoken=self.Authkey, rows=5, sort_order="desc")

            # delete existing records for the fullid
            self.deleteRecordsForFullid(fullid, table_name)

            rdf = pandas.DataFrame(revData)
            pdf = pandas.DataFrame(profitData)

            pmdf = pandas.DataFrame(profitMarginData)
            opdf = pandas.DataFrame(operatingProfitData)
            opmdf = pandas.DataFrame(operatingProfitMarginData)
            ebidtdf = pandas.DataFrame(ebidtData)
            ebidtmdf = pandas.DataFrame(ebidtMarginData)

            rdata = rdf.to_dict(orient='dict')
            pdata = pdf.to_dict(orient='dict')

            pmdata = pmdf.to_dict(orient='dict')
            opdata = opdf.to_dict(orient='dict')
            opmdata = opmdf.to_dict(orient='dict')
            ebidtdata = ebidtdf.to_dict(orient='dict')
            ebidtmdata = ebidtmdf.to_dict(orient='dict')


            # pprint(data3["CONSOLIDATED"])
            if(type == "C"):
                rdataC = rdata['CONSOLIDATED']
                pdataC = pdata['CONSOLIDATED']

                pmdataC = pmdata['CONSOLIDATED']
                opdataC = opdata['CONSOLIDATED']
                opmdataC = opmdata['CONSOLIDATED']
                ebidtdataC = ebidtdata['CONSOLIDATED']
                ebidtmdataC = ebidtmdata['CONSOLIDATED']

                firstkey = rdataC.keys()[0]
                # print firstkey
                checkData = rdataC[firstkey]
                if(math.isnan(checkData)):
                    rdataC = rdata['STANDALONE']
                    pdataC = pdata['STANDALONE']

                    pmdataC = pmdata['STANDALONE']
                    opdataC = opdata['STANDALONE']
                    opmdataC = opmdata['STANDALONE']
                    ebidtdataC = ebidtdata['STANDALONE']
                    ebidtmdataC = ebidtmdata['STANDALONE']

                    print "\nAmit, changing type for this stock to Standalone\n"
                    updateSql = "update stock_names set data_type = 'S' where fullid = '%s' " % fullid
                    self.cur.execute(updateSql)
                    self.con.commit()


            else:
                rdataC = rdata['STANDALONE']
                pdataC = pdata['STANDALONE']
                pmdataC = pmdata['STANDALONE']
                opdataC = opdata['STANDALONE']
                opmdataC = opmdata['STANDALONE']
                ebidtdataC = ebidtdata['STANDALONE']
                ebidtmdataC = ebidtmdata['STANDALONE']

            insert_sql = ("INSERT INTO " + table_name + " (nseid,fullid, quater_sequence, period,quater_name,  revenueC, profitC,  profit_margin, opmC, operating_profit_margin, ebidtaC, ebidt_margin, last_modified, created_on ) VALUES (%s,%s, %s, %s, %s,%s, %s, %s,%s, %s,%s,%s, %s,%s )")

            rSize = len(rdataC)
            pSize = len(pdataC)
            if(rSize != pSize):
                print 'Revenue and Profit data not same size'

            count = rSize;

            try:
                for key in reversed(sorted(rdataC.keys())):

                    #now = datetime.date.today();
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dt = key.to_datetime()
                    date = dt.strftime("%Y-%m-%d")

                    # print date
                    quarter = self.quater_names[date]
                    # print quarter
                    rev = float("{0:.2f}".format(rdataC[key]))
                    profit = float("{0:.2f}".format(pdataC[key]))


                    profitMargin = float("{0:.4f}".format(pmdataC[key]))*100
                    operatingProfit = float("{0:.2f}".format(opdataC[key]))
                    operatingProfitMargin  = float("{0:.4f}".format(opmdataC[key]))*100
                    ebidt = float("{0:.2f}".format(ebidtdataC[key]))
                    ebidtm  = float("{0:.4f}".format(ebidtmdataC[key]))*100

                    data_quater = (nseid, fullid, count, date,quarter, rev, profit,profitMargin, operatingProfit, operatingProfitMargin, ebidt, ebidtm,  now, now)
                    self.cur.execute(insert_sql, data_quater)
                    print "Insert executed for table " + table_name + ", date -  ", date
                    count = count - 1;
            except  Exception, e:
                print "error e - ",str(e)
                print "\n******Amit - some excetion executing " + table_name + " insert sql 111 for  - "+nseid
                self.qd_exception_list.append(nseid)
                self.all_good_flag = False
                pass

            self.con.commit()
        except  Exception, e2:
            print "error e2 - ",str(e2)
            print "\n******Amit - some excetion executing " + table_name + " insert sql 222 for  - " + nseid
            self.qd_exception_list.append(nseid)
            self.all_good_flag = False
            pass


    def updateFinancialRatios(self, row,table_name):


        Vendor_Name = 'DEB'
        eps_ds_suffix = '_A_EPS'
        pe_ds_suffix = '_A_PE'
        pb_ds_suffix = '_A_PBV'
        roe_ds_suffix = '_A_ROE'
        debt_ds_suffix = '_A_DEBT'
        interest_ds_suffix = '_A_INT'
        interest_coverage_ds_suffix = '_A_IC'
        debt_equity_ds_suffix = '_A_LTDE'


        # words = nseidType.split(",")
        nseid = row["nseid"]
        nseidModified = nseid.replace("&", "")
        type = row["data_type"]

        fullid = "NSE:"+nseid

        short_name = self.getShortName(fullid,table_name)

        # delete existing records for the fullid
        self.deleteRecordsForFullid(fullid, table_name)

        try:
            #nse = Nse()
            #q = nse.get_quote(str(nseid))
            nseDict = NSELiveDataModule.getNSELiveData(nseidModified)
            # print q
            high52 = nseDict['high52']
            low52 = nseDict['low52']
            #companyName = q['companyName']
            last_price = nseDict['last_price']
            print nseid, " high 52 -",  high52
        except Exception, e3:
            print str(e3)
            print "\n******Amit - Exception in getting NSE data for - " + nseid
            self.fr_exception_list.append(nseid)
            return

        #nseidModified = nseid.replace("&", "")

        eps_dataset = Vendor_Name+"/"+nseidModified+eps_ds_suffix
        pe_dataset = Vendor_Name + "/" + nseidModified + pe_ds_suffix
        pb_dataset = Vendor_Name+"/"+nseidModified+pb_ds_suffix
        roe_dataset = Vendor_Name + "/" + nseidModified + roe_ds_suffix
        debt_dataset = Vendor_Name+"/"+nseidModified+debt_ds_suffix
        int_dataset = Vendor_Name + "/" + nseidModified + interest_ds_suffix
        ic_dataset = Vendor_Name+"/"+nseidModified+interest_coverage_ds_suffix
        de_dataset = Vendor_Name + "/" + nseidModified + debt_equity_ds_suffix

        eps_exception_flag = False
        pe_exception_flag = False
        pb_exception_flag = False
        roe_exception_flag = False
        debt_exception_flag = False
        int_exception_flag = False
        ic_exception_flag = False
        de_exception_flag = False

        #all_good_flag = True

        try:
            try:
                epsData = quandl.get(eps_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                epsdata2 = pandas.DataFrame(epsData).to_dict(orient='dict')

                if (type == "C"):
                    epsdataC = epsdata2['CONSOLIDATED']
                else:
                    epsdataC = epsdata2['STANDALONE']
            except:
                eps_exception_flag = True
                eps = 0
                pass

            try:
                peData = quandl.get(pe_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                pedata2 = pandas.DataFrame(peData).to_dict(orient='dict')
                if (type == "C"):
                    pedataC = pedata2['CONSOLIDATED']
                else:
                    pedataC = pedata2['STANDALONE']
            except:
                pe_exception_flag = True
                pe = 0
                pass

            try:
                pbData = quandl.get(pb_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                pbdata2 = pandas.DataFrame(pbData).to_dict(orient='dict')
                if (type == "C"):
                    pbdataC = pbdata2['CONSOLIDATED']
                else:
                    pbdataC = pbdata2['STANDALONE']
            except:
                pb_exception_flag = True
                pb = 0
                pass

            try:
                roeData = quandl.get(roe_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                roedata2 = pandas.DataFrame(roeData).to_dict(orient='dict')

                if (type == "C"):
                    roedataC = roedata2['CONSOLIDATED']
                else:
                    roedataC = roedata2['STANDALONE']
            except:
                roe_exception_flag = True
                roe = 0
                pass

            try:
                debtData = quandl.get(debt_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                debtdata2 = pandas.DataFrame(debtData).to_dict(orient='dict')
                if (type == "C"):
                    debtdataC = debtdata2['CONSOLIDATED']
                else:
                    debtdataC = debtdata2['STANDALONE']
            except:
                debt_exception_flag = True
                debt = 0
                pass

            try:
                intData = quandl.get(int_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                intdata2 = pandas.DataFrame(intData).to_dict(orient='dict')
                if (type == "C"):
                    intdataC = intdata2['CONSOLIDATED']
                else:
                    intdataC = intdata2['STANDALONE']
            except:
                int_exception_flag = True
                int = 0
                pass

            try:
                icData = quandl.get(ic_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                icdata2 = pandas.DataFrame(icData).to_dict(orient='dict')
                if (type == "C"):
                    icdataC = icdata2['CONSOLIDATED']
                else:
                    icdataC = icdata2['STANDALONE']
            except:
                ic_exception_flag = True
                ic = 0
                pass

            try:
                deData = quandl.get(de_dataset, authtoken=self.Authkey, rows=1, sort_order="desc")
                dedata2 = pandas.DataFrame(deData).to_dict(orient='dict')
                if (type == "C"):
                    dedataC = dedata2['CONSOLIDATED']
                else:
                    dedataC = dedata2['STANDALONE']
            except:
                de_exception_flag = True
                de = 0
                pass




            insert_sql = ("INSERT INTO " + table_name + " (fullid, short_name,last_price, eps_ttm,pe,  pb, roe,52_week_high, 52_week_low,last_price_vs_high52, debt,interest, interest_cover, debt_equity_ratio, last_modified, created_on ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s,%s ,%s, %s, %s,%s, %s,%s)")
            # insert_sql = ("INSERT INTO " + table_name + " (fullid, short_name, eps_ttm,52_week_high, 52_week_low,last_modified, created_on ) VALUES (%s, %s, %s, %s,%s,%s, %s)")


            try:
                for key in reversed(sorted(epsdataC.keys())):

                    #now = datetime.date.today();
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    if (not eps_exception_flag):
                        eps = float("{0:.2f}".format(epsdataC.get(key, 0)))
                    if (not pe_exception_flag):
                        pe = float("{0:.2f}".format(pedataC.get(key, 0)))
                    if (not pb_exception_flag):
                        pb = float("{0:.2f}".format(pbdataC.get(key, 0)))
                    if (not roe_exception_flag):
                        roe = float("{0:.2f}".format(roedataC.get(key, 0) * 100)) # convert in percentage
                    if (not debt_exception_flag):
                        debt = float("{0:.2f}".format(debtdataC.get(key, 0)))
                    if (not int_exception_flag):
                        int = float("{0:.2f}".format(intdataC.get(key, 0)))
                    if (not ic_exception_flag):
                        ic = float("{0:.2f}".format(icdataC.get(key, 0)))
                    if (not de_exception_flag):
                        de = float("{0:.2f}".format(dedataC.get(key, 0)))

                    high52 = float("{0:.2f}".format(high52))
                    low52 = float("{0:.2f}".format(low52))
                    last_price = float("{0:.2f}".format(last_price))
                    last_price_vs_high52 = (last_price / high52) * 100
                    last_price_vs_high52 = float("{0:.2f}".format(last_price_vs_high52))
                    print "last_price_vs_high52 - ", last_price_vs_high52

                    # data_quater = (fullid, "", eps, high52, low52,now, now)
                    data_quater = (fullid, short_name,last_price, eps, pe, pb, roe, high52, low52, last_price_vs_high52,debt, int, ic, de, now, now)
                    self.cur.execute(insert_sql, data_quater)
                    print "eps-",eps," | pe-",pe," | pb-",pb," | roe-",roe," | low52-",low52
                    print "Insert executed for table " + table_name + ", key -  ", key



            except Exception, e:
                print "error e - ", str(e)
                print "\n******Amit - some excetion executing " + table_name + " insert sql 111 for  - "+nseid+" and type - "+type
                self.fr_exception_list.append(nseid)
                self.all_good_flag = False
                pass



        except  Exception, e2:
            print "error e2 - ", str(e2)
            print "\n******Amit - some excetion executing " + table_name + " insert sql 222 for  - " + nseid
            self.fr_exception_list.append(nseid)
            self.all_good_flag = False
            pass

        # Since everything went fine, update the 'update_now' flag to c
        if(self.all_good_flag):
            self.setUpdateNowFlag(fullid,table_name, 'c' )

#        else:
            #updateSql = "update stock_names set update_now = 'e' where fullid = '%s' " % fullid
            #self.cur.execute(updateSql)
            #self.con.commit()
            #print "updated the update_now column to e (ERROR)"



    def getShortName(self, fullid,table_name ):

        #get the short name since it is manually modified
        #selectSql = "select short_name from " + table_name + " where fullid = '%s' " % fullid
        selectSql = "select fr.short_name, sn.name from   " + table_name + " fr, stock_names sn where fr.fullid = sn.fullid and fr.fullid = '%s' " % fullid

        self.cur.execute(selectSql)
        short_name = ""
        rows = self.cur.fetchall()
        for row in rows:
            short_name = row[0]
            if(short_name == ""):
                short_name = row[1]
            print "short_name - ",short_name

        return short_name

    def deleteRecordsForFullid(self, fullid, table_name):
        # delete existing records for the fullid
        deleteSql = "delete from " + table_name + " where fullid = '%s' " % fullid
        self.cur.execute(deleteSql)
        self.con.commit()
        print "Number of rows delete: %d" % self.cur.rowcount
        return self.cur.rowcount

    def setUpdateNowFlag(self, fullid,table_name, update_flag ):

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if "secondary" in table_name:
            vendor_data_flag = "2"
        elif "us_stocks" in table_name:
            vendor_data_flag = "3"
        else:
            vendor_data_flag = "1"
        updateSql = "update stock_names set update_now = '%s', enable_for_vendor_data='%s', last_modified='%s' where fullid = '%s' " % (
            update_flag, vendor_data_flag, now, fullid)
        #print updateSql
        self.cur.execute(updateSql)
        self.con.commit()
        #self.updated_stock_list.append(fullid)
        print "updated the update_now column to ", update_flag

    def setIsVideoAvailable(self, fullid ):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        updateSql = "update stock_names set is_video_available = 'y', last_modified='%s' where fullid = '%s' " % ( now, fullid)
        #print updateSql
        self.cur.execute(updateSql)
        self.con.commit()
        print "updated the is_video_available column to y"

    def setVideoAsOldToRecreateNextTime(self, fullid ):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        updateSql = "update stock_recommended_videos set last_modified = '2000-01-01 00:00:00'  where fullid = '%s' " % ( fullid)
        #print updateSql
        self.cur.execute(updateSql)
        self.con.commit()
        print "updated the stock_recommended_videos:last_updated column to old date"


    def __del__(self):
        self.cur.close()
        self.con.close()

