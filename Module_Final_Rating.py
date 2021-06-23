from decimal import Decimal

import DBManager
import time
import EmailUtil
#import itertools
import datetime
import QuandlDataModule
import Constants as C
import operator
import ModuleAmitException


class Module_Final_Rating:
    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.compare_percentage_1 = 5
        self.compare_percentage_2 = 10
        self.compare_percentage_3 = 20
        self.compare_percentage_negative_1 = -10
        #self.compare_percentage_negative_2 = -20

        self.max_final_rating = None  # This will be set to max with every stock calculation and then used for calibration

        self.no_of_quarter_to_comapre = 4 #  if you change this then you need to change the weigtage for row, ic, de
        #self.rating_total = self.no_of_quarter_to_comapre * (1+2+2+2+2+.5+.5+.5)
        self.rating_total = Decimal(self.no_of_quarter_to_comapre * 2 *(3* C.genericW+ 2*C.opmANDebitW+C.roeW+C.icW+C.deW))
        #print self.rating_total
        #self.scrapper_exception_list = []
        self.sql_exception_list = []
        self.updated_stock_list = []
        self.qData_missing_stock_list = []

        self.ratingDict = {}

        self.quandlDataObject = QuandlDataModule.QuandlDataModule()

    # def __del__(self):
    #
    #     # self.con.close
    #     # self.cur.close()
    #     print "\n\n*****  deleting Module_Final_Rating "


    def getMaxFinalRating(self):
        select_sql = "select max(percentage_rating) max_rating from stocksdb.final_rating "

        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        maxR = None
        for row in rows:
            maxR = row[0]

        return maxR

    def getStockList(self):
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        select_sql = "select fullid, nseid from stocksdb.stock_names_new sn where exchange='NSE' and is_video_available='y' "
        self.cur.execute(select_sql)

        rows = self.cur.fetchall()
        data = list()
        for row in rows:
            dd = dict()
            dd["fullid"] = row[0]
            dd["nseid"] = row[1]
            dd["enable_for_vendor_data"] = row[2]
            data.append(dd)
        return data

    def getQuartetlyData(self, fullid, table_name):
        quater_seq_level = 5-self.no_of_quarter_to_comapre
        select_sql = "select nseid,fullid,quater_sequence,revenue_growth_rate, profit_growth_rate,profit_margin,operating_profit_margin,ebidt_margin  from "+table_name+"" \
                    " qd where fullid ='%s' and quater_sequence > '%s' " %(fullid,quater_seq_level )
        self.cur.execute(select_sql)

        #rows = self.cur.fetchall()

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def getFinancialRatioData(self, fullid, table_name):
        quater_seq_level = 5-self.no_of_quarter_to_comapre
        select_sql = "select fullid, roe,profit_ind, revenue_ind, interest_cover, debt_equity_ratio from "+table_name+"" \
                    " fr where fullid ='%s' " %(fullid )
        self.cur.execute(select_sql)

        #rows = self.cur.fetchall()
        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data


    def updateFinalRatingTempData(self, row,qd_table_name,fr_table_name):
        try:
            self.all_good_flag = True
            nseid = row["nseid"]
            fullid = row["fullid"]
            industry_vertical = row["industry_vertical"]

            print( 'fullid - ',fullid)


            qData = self.getQuartetlyData(fullid, qd_table_name)
            frData = self.getFinancialRatioData(fullid, fr_table_name)

            if len(qData) == 0:
                self.all_good_flag = False
                self.qData_missing_stock_list.append(nseid)
                print( "*** Amit - qData is zero, check the table why qData missing. For - ",nseid)
                return

            #records = []
            if industry_vertical == 'Bank':
                records = self.processBankSectorData(row,frData, qData)
            else:
                records = self.processGenericSectorData(row,frData, qData)

            #delete earlier records
            delete_sql = "delete from final_rating_temp where fullid = '%s'" % fullid
            self.cur.execute(delete_sql)
            self.con.commit()

            #print records
            records_list_template = ','.join(['%s'] * len(records))
            insert_sql = "insert into final_rating_temp (fullid, quater_sequence, revenue,profit,op_profit,ebit, profit_margin,roe,interest_cover,debt_equity_ratio,last_modified, created_on)" \
                             " values (%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s) "
            self.cur.executemany(insert_sql, records)
            self.con.commit()



        except Exception as e2:
            print( str(e2))
            print( "\n******Amit - Exception in inserting final_rating_temp data for - " + fullid)
            ModuleAmitException.printInfo()
            self.sql_exception_list.append(nseid)
            self.all_good_flag = False
            pass


    def processGenericSectorData(self,row,frData, qData):

        nseid = row["nseid"]
        fullid = row["fullid"]

        records=[]
        # print qData
        print( frData)
        icI = deI = roeI = 0
        for list_item in frData:
            ic = list_item['interest_cover']
            de = list_item['debt_equity_ratio']
            roe = list_item['roe']

            if roe is None or roe == '':
                roeI = 0  # beyond 50 there are industry 'type' and other reasons for high ROE
            elif roe > 50:
                roeI = 1  # beyond 50 there are industry 'type' and other reasons for high ROE
            elif roe > 20:
                roeI = 2
            elif roe > 10:
                roeI = 1
            elif roe > 0:
                roeI = 0
            elif roe < 0:
                roeI = -1
            else:
                roeI = 0

            if ic is None or ic > 2 :
                icI = 2
            elif ic > 1:
                icI = 1
            else:
                icI = 0

            if de is None or de == '':
                deI = 2  
            elif de > 2 or de < 0:  # negative de means equity is negative which is not good for company
                deI = 0
            elif de > 1:
                deI = 1
            else:
                deI = 2

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for list_item in qData:
            print (list_item)
            revI = profitI = pmI = opmI = ebitI = 0
            quater_sequence = list_item['quater_sequence']
            revG = list_item['revenue_growth_rate']
            profitG = list_item['profit_growth_rate']
            pmG = list_item['profit_margin']
            opmG = list_item['operating_profit_margin']
            ebitG = list_item['ebidt_margin']

            if revG > self.compare_percentage_3:
                revI = 2
            elif revG > self.compare_percentage_2:
                revI = 1
            elif revG > self.compare_percentage_1:
                revI = .5
            elif revG < self.compare_percentage_negative_1:
                revI = -1
            else:
                revI = 0

            if profitG > self.compare_percentage_3:
                profitI = 2
            elif profitG > self.compare_percentage_2:
                profitI = 1
            elif profitG > self.compare_percentage_1:
                profitI = .5
            elif profitG < self.compare_percentage_negative_1:
                profitI = -1
            else:
                profitI = 0

            if pmG > self.compare_percentage_3:
                pmI = 2
            elif pmG > self.compare_percentage_2:
                pmI = 1
            elif pmG > self.compare_percentage_1:
                pmI = .5
            elif pmG < self.compare_percentage_negative_1:
                pmI = -1
            else:
                pmI = 0

            if opmG > self.compare_percentage_3:
                opmI = 2
            elif opmG > self.compare_percentage_2:
                opmI = 1
            elif opmG > self.compare_percentage_1:
                opmI = .5
            elif opmG < self.compare_percentage_negative_1:
                opmI = -1
            else:
                opmI = 0

            if ebitG > self.compare_percentage_3:
                ebitI = 2
            elif ebitG > self.compare_percentage_2:
                ebitI = 1
            elif ebitG > self.compare_percentage_1:
                ebitI = .5
            elif ebitG < self.compare_percentage_negative_1:
                ebitI = -1
            else:
                ebitI = 0

            records.append((fullid, quater_sequence, revI, profitI, opmI, ebitI, pmI, roeI, icI, deI, now, now))
        return records

    def processBankSectorData(self, row, frData, qData):


        nseid = row["nseid"]
        fullid = row["fullid"]

        records=[]
        # print qData
        print( frData)
        icI = deI = roeI = 0
        for list_item in frData:
            ic = list_item['interest_cover']
            de = list_item['debt_equity_ratio']
            roe = list_item['roe']

            #Bank normally has Max ROE of 20, mostly in te range of 5-20. Anything above 10 is good for Banks.
            #
            if roe > 10:
                roeI = 2
            elif roe > 5:
                roeI = 1
            elif roe < -10:
                roeI = -1
            else:
                roeI = 0
            #IC will be always low because Bank has big debt (that what they do, take money from RBI etc and lend to people) and hence interest would be high.
            #Anything above 1.5 is v good.

            if ic > 1.5 :
                icI = 2
            elif ic > 1:
                icI = 1
            else:
                icI = 0
            #D E ratio will be very high for banks because of high debt as mentioned above. 5-30 is typical ratio. Good banks will have in the range 4-10.
            if de > 20 :  # negative de means equity is negative which is not good for company
                deI = 0
            elif de > 10:
                deI = 1
            elif de > 0:
                deI = 2
            else:
                deI = 0

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for list_item in qData:
            # print list_item
            revI = profitI = pmI = opmI = ebitI = 0
            quater_sequence = list_item['quater_sequence']
            revG = list_item['revenue_growth_rate']
            profitG = list_item['profit_growth_rate']
            pmG = list_item['profit_margin']
            opmG = list_item['operating_profit_margin']
            ebitG = list_item['ebidt_margin']

            #Typically a Bank has Revenuw Growth rate uo to 10%.
            if revG > 5:
                revI = 2
            elif revG > 0:
                revI = 1
            elif revG < -5:
                revI = -1
            else:
                revI = 0

            # Typically a Bank has Profit Growth rate 0-15%.
            if profitG > 10:
                profitI = 2
            elif profitG > 5:
                profitI = 1
            elif profitG > 0:
                profitI = .5
            elif profitG < -10 :
                profitI = -1
            else:
                profitI = 0

            if pmG > 20:
                pmI = 2
            elif pmG > 10:
                pmI = 1
            elif pmG > 5:
                pmI = .5
            elif pmG < -10 :
                pmI = -1
            else:
                pmI = 0
            #Banks has high operating Margins in the range 20-80%
            if opmG > 50:
                opmI = 2
            elif opmG > 25:
                opmI = 1
            elif opmG > 10:
                opmI = .5
            elif opmG < -10:
                opmI = -1
            else:
                opmI = 0

            if ebitG > 20:
                ebitI = 2
            elif ebitG > 10:
                ebitI = 1
            elif ebitG > 5:
                ebitI = .5
            elif ebitG < -10:
                ebitI = -1
            else:
                ebitI = 0

            records.append((fullid, quater_sequence, revI, profitI, opmI, ebitI, pmI, roeI, icI, deI, now, now))
        return records




    def updateFinalRatingData(self, row ,qd_table_name,fr_table_name):
        try:
            nseid = row["nseid"]
            fullid = row["fullid"]

            print( 'fullid - ',fullid)
            
            select_sql = "select * from stocksdb.final_rating_temp where fullid ='%s' " %(fullid)
            self.cur.execute(select_sql)

            # rows = self.cur.fetchall()

            desc = self.cur.description
            column_names = [col[0] for col in desc]
            frData = [dict(zip(column_names, row))
                    for row in self.cur.fetchall()]

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            revT = profitT = pmT = opmT = ebitT = icT = deT = roeT=  Decimal(0.0)
            for list_item in frData:
                revenue = list_item['revenue']
                profit = list_item['profit']
                op_profit = list_item['op_profit']
                ebit = list_item['ebit']
                profit_margin = list_item['profit_margin']
                roe = list_item['roe']
                interest_cover = list_item['interest_cover']
                debt_equity_ratio = list_item['debt_equity_ratio']


                revT += revenue
                profitT += profit
                opmT += op_profit
                pmT += profit_margin
                ebitT += ebit
                roeT += roe
                icT += interest_cover
                deT += debt_equity_ratio


            #Apply weightages
            # revT = revT*Decimal(C.revenueW)
            # roeT = roeT*Decimal(C.roeW)
            # icT = icT*Decimal(C.icW)
            # deT = deT*Decimal(C.deW)

            # delete earlier records
            delete_sql = "delete from final_rating where fullid = '%s'" % fullid
            self.cur.execute(delete_sql)
            self.con.commit()

            #get the latest quarter for which result exists
            select_sql = "select quater_name FROM "+qd_table_name+" where quater_sequence=5 and fullid='%s' " %(fullid)
            self.cur.execute(select_sql)

            rows = self.cur.fetchall()
            latest_quarter = None
            data = list()
            for row in rows:
                latest_quarter = row[0]

            print( "latest_quarter- ",latest_quarter)
            #total = Decimal(revT+profitT+opmT+pmT+ebitT+roeT+icT+deT)
            total = Decimal(revT + profitT + pmT + (opmT +ebitT)*Decimal(C.opmANDebitW) + roeT*Decimal(C.roeW) + icT*Decimal(C.icW) + deT*Decimal(C.deW))
            percentage_rating = '{0:.2f}'.format((total/self.rating_total)*10)

            print( " Final Rating", percentage_rating)
            self.ratingDict[fullid+"["+latest_quarter+"]"] = percentage_rating
            data = (fullid, percentage_rating,latest_quarter, revT, profitT, opmT, pmT, ebitT, roeT, icT, deT,total, now, now )
            insert_sql = "insert into final_rating (fullid, percentage_rating, latest_quarter, revenue,profit,op_profit,ebit, profit_margin,roe, interest_cover,debt_equity_ratio, total, last_modified, created_on)" \
                         " values (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s) "
            self.cur.execute(insert_sql, data)
            self.con.commit()

        except Exception as e2:
            print( str(e2))
            print( "\n******Amit - Exception in inserting final_rating data for - " + fullid)
            ModuleAmitException.printInfo()
            self.sql_exception_list.append(nseid)
            self.all_good_flag = False
            pass


        if (self.all_good_flag):
            # self.quandlDataObject.setUpdateNowFlag(fullid, qd_table_name, 'n')
            # self.quandlDataObject.setIsVideoAvailable(fullid)
            # self.quandlDataObject.setVideoAsOldToRecreateNextTime(fullid)
            self.updated_stock_list.append(nseid)


    def updateFinalRating(self,row,qd_table_name,fr_table_name ):
        self.updateFinalRatingTempData(row,qd_table_name,fr_table_name)
        if (self.all_good_flag):
            self.updateFinalRatingData(row,qd_table_name,fr_table_name)
        else:
            print( "*** Amit -  Since updateFinalRatingTempData FAILED , not calling updateFinalRatingData.Move to next one ")


    def calibrateAllRatings(self, stock_names = None):

        self.max_final_rating = Decimal(self.getMaxFinalRating())
        if stock_names is None:
            select_sql = "select fullid, percentage_rating from stocksdb.final_rating "
            self.cur.execute(select_sql)
            rows = self.cur.fetchall()
        else:
            fullidList = " where fullid in ( "
            for row in stock_names:
                fullidList += "'"+row["fullid"]+"',"

            fullidList = fullidList[:-1]
            fullidList += " )"
            print( fullidList)
            select_sql = "select fullid, percentage_rating from stocksdb.final_rating " + fullidList
            self.cur.execute(select_sql)
            rows = self.cur.fetchall()

        for row in rows:
            fullid = row[0]

            percentage_rating = Decimal(row[1])
            print( "Before Calibration Rating for ",fullid," - ", percentage_rating, ",  max rating - ", self.max_final_rating)
            # if percentage_rating > self.max_final_rating:
            #     self.max_final_rating = percentage_rating

            # Calibrate this with the max rating so that is does not look bad..
            percentage_rating = (percentage_rating * 10) / self.max_final_rating
            percentage_rating = '{0:.2f}'.format(percentage_rating)  # 7*2*3  seven param * point * rows
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            updateSql = "update final_rating set percentage_rating = '%s', last_modified='%s' where fullid = '%s' " % (
                percentage_rating,now, fullid)
            self.cur.execute(updateSql)
            print( "updated Calibrated Final Rating to - ", percentage_rating)

        self.con.commit()

    def updateAll(self,stock_names ):
        start_time = time.time()
        print( stock_names)
        print( "Number of Stocks processing - " , len(stock_names))
        totalCount = len(stock_names)
        count = 0
        for row in stock_names:
            # print row
            count = count + 1
            print( "\n\ncalling updateFinalRatingData for - ", row['nseid'], "(", count, "/", totalCount, ")")
            # enable_for_vendor_data = row['enable_for_vendor_data']
            # if enable_for_vendor_data == '1':
            qd_table_name = "fa_quaterly_data"
            fr_table_name = "fa_financial_ratio"
            self.updateFinalRating(row,qd_table_name,fr_table_name)
            # elif enable_for_vendor_data == '2':
            #     qd_table_name = "fa_quaterly_data_secondary"
            #     fr_table_name = "fa_financial_ratio_secondary"
            #     self.updateFinalRating(row, qd_table_name, fr_table_name)

        # Now calibrate all rating with respect to max rating
        #commented because each stock becomes depended on other stocks rating and if few stock rating changes then you have tp calculate all stocsk again
        #self.calibrateAllRatings(stock_names)

        print( "\n\n sql_exception_list  - ")
        print( self.sql_exception_list)

        print( "\n Updated Stock list for - ", len(self.updated_stock_list), " Stocks")
        print( self.updated_stock_list)

        print( "\n qData_missing_stock_list list for - ", len(self.qData_missing_stock_list), " Stocks")
        print( self.qData_missing_stock_list)

        self.ratingDict = sorted(self.ratingDict.items(), key=operator.itemgetter(1))
        print( "\n Final rating Dictionary - ", self.ratingDict)

        print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )
        url = "http://localhost/stockcircuitserver/php/report_two_quarters_compare.php"
        EmailUtil.send_email_as_text("Final rating Dictionary - ",self.ratingDict, url)

        


