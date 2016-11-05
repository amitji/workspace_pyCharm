from decimal import Decimal

import DBManager
import time
import EmailUtil
import itertools
import datetime
import QuandlDataModule
import Constants as C


class FinalRatingModule:
    def __init__(self):
        # print "Calling parent constructor"
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.compare_percentage_1 = 5
        self.compare_percentage_2 = 10
        self.compare_percentage_3 = 20
        self.compare_percentage_negative_1 = -10
        #self.compare_percentage_negative_2 = -20

        self.no_of_quarter_to_comapre = 4 #  if you change this then you need to change the weigtage for row, ic, de
        #self.rating_total = self.no_of_quarter_to_comapre * (1+2+2+2+2+.5+.5+.5)
        self.rating_total = Decimal(self.no_of_quarter_to_comapre * 2 *(3* C.genericW+ 2*C.opmANDebitW+C.roeW+C.icW+C.deW))
        #print self.rating_total
        #self.scrapper_exception_list = []
        self.sql_exception_list = []
        self.updated_stock_list = []
        self.qData_missing_stock_list = []

        self.quandlDataObject = QuandlDataModule.QuandlDataModule()

    def getStockList(self):
        #select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and update_now='y' "
        select_sql = "select fullid, nseid, enable_for_vendor_data from stocksdb.stock_names sn where exchange='NSE' and is_video_available='y' "
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
        data = [dict(itertools.izip(column_names, row))
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
        data = [dict(itertools.izip(column_names, row))
                for row in self.cur.fetchall()]
        return data


    def updateFinalRatingTempData(self, row,qd_table_name,fr_table_name):
        try:
            self.all_good_flag = True
            nseid = row["nseid"]
            fullid = row["fullid"]
            # fullid = 'NSE:INFY'
            # nseid = 'INFY'
            print fullid


            qData = self.getQuartetlyData(fullid, qd_table_name)
            frData = self.getFinancialRatioData(fullid, fr_table_name)

            if len(qData) == 0:
                self.all_good_flag = False
                self.qData_missing_stock_list.append(nseid)
                print "*** Amit - qData is zero, check the table why qData missing. For - ",nseid
                return

            # print qData
            print frData
            icI=deI=roeI=0
            for list_item in frData:
                ic = list_item['interest_cover']
                de = list_item['debt_equity_ratio']
                roe = list_item['roe']

                if roe > 50:
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

                if ic > 2 or ic is None:
                    icI = 2
                elif ic > 1:
                    icI = 1
                else:
                    icI = 0

                if de > 2 or de < 0:   # negative de means equity is negative which is not good for company
                    deI = 0
                elif de > 1:
                    deI = 1
                else:
                    deI = 2

            records= []
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for list_item in qData:
                    #print list_item
                    revI= profitI=pmI= opmI= ebitI = 0
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


                    records.append((fullid,quater_sequence,revI,profitI, opmI,ebitI,pmI,roeI,icI,deI, now, now))
                    # insert_sql = ("INSERT INTO final_rating_temp (fullid, revenue,profit,op_profit,ebit, profit_margin) VALUES (%s,%s, %s,%s,%s,%s) " )
                    # data = (fullid,revI,profitI,opmI,ebitI, pmI)
                    # print data
                    # self.cur.execute(insert_sql, data)
                    # self.con.commit()

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



        except Exception, e2:
            print str(e2)
            print "\n******Amit - Exception in inserting final_rating_temp data for - " + fullid
            self.sql_exception_list.append(nseid)
            self.all_good_flag = False
            pass



    def updateFinalRatingData(self, row ,qd_table_name,fr_table_name):
        try:
            nseid = row["nseid"]
            fullid = row["fullid"]

            select_sql = "select * from stocksdb.final_rating_temp where fullid ='%s' " %(fullid)
            self.cur.execute(select_sql)

            # rows = self.cur.fetchall()

            desc = self.cur.description
            column_names = [col[0] for col in desc]
            frData = [dict(itertools.izip(column_names, row))
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


            #total = Decimal(revT+profitT+opmT+pmT+ebitT+roeT+icT+deT)
            total = Decimal(revT + profitT + pmT + (opmT +ebitT)*Decimal(C.opmANDebitW) + roeT*Decimal(C.roeW) + icT*Decimal(C.icW) + deT*Decimal(C.deW))
            percentage_rating = '{0:.3g}'.format((total/self.rating_total)*10)  # 7*2*3  seven param * point * rows
            print "Final Rating", total, "/", self.rating_total, "->", percentage_rating
            data = (fullid, revT, profitT, opmT, pmT, ebitT, roeT, icT, deT,total, percentage_rating, now, now )
            insert_sql = "insert into final_rating (fullid, revenue,profit,op_profit,ebit, profit_margin,roe, interest_cover,debt_equity_ratio, total, percentage_rating, last_modified, created_on)" \
                         " values (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s) "
            self.cur.execute(insert_sql, data)
            self.con.commit()

        except Exception, e2:
            print str(e2)
            print "\n******Amit - Exception in inserting final_rating data for - " + fullid
            self.sql_exception_list.append(nseid)
            self.all_good_flag = False
            pass


        if (self.all_good_flag):
            self.quandlDataObject.setUpdateNowFlag(fullid, qd_table_name, 'n')
            self.quandlDataObject.setIsVideoAvailable(fullid)
            self.quandlDataObject.setVideoAsOldToRecreateNextTime(fullid)
            self.updated_stock_list.append(nseid)


    def updateFinalRating(self,row,qd_table_name,fr_table_name ):
        self.updateFinalRatingTempData(row,qd_table_name,fr_table_name)
        if (self.all_good_flag):
            self.updateFinalRatingData(row,qd_table_name,fr_table_name)
        else:
            print "*** Amit -  Since updateFinalRatingTempData FAILED , not calling updateFinalRatingData.Move to next one "

    def updateAll(self,stock_names ):
        start_time = time.time()
        print stock_names
        print "Number of Stocks processing - " , len(stock_names)
        totalCount = len(stock_names)
        count = 0
        for row in stock_names:
            # print row
            count = count + 1
            print "\n\ncalling updateFinalRatingData for - ", row['nseid'], "(", count, "/", totalCount, ")"
            enable_for_vendor_data = row['enable_for_vendor_data']
            if enable_for_vendor_data == '1':
                qd_table_name = "fa_quaterly_data"
                fr_table_name = "fa_financial_ratio"
                self.updateFinalRating(row,qd_table_name,fr_table_name)
                # thisObj.updateFinalRatingTempData(row,qd_table_name,fr_table_name)
                # if (thisObj.all_good_flag):
                #     thisObj.updateFinalRatingData(row,qd_table_name,fr_table_name)
                # else:
                #     print "*** Amit -  Since updateFinalRatingTempData FAILED , not calling updateFinalRatingData.Move to next one "
            elif enable_for_vendor_data == '2':
                qd_table_name = "fa_quaterly_data_secondary"
                fr_table_name = "fa_financial_ratio_secondary"
                self.updateFinalRating(row, qd_table_name, fr_table_name)
                # thisObj.updateFinalRatingTempData(row,qd_table_name,fr_table_name)
                # if (thisObj.all_good_flag):
                #     thisObj.updateFinalRatingData(row, qd_table_name, fr_table_name)
                # else:
                #     print "*** Amit -  Since updateFinalRatingTempData FAILED , not calling updateFinalRatingData.Move to next one "



        print "\n\n sql_exception_list  - "
        print self.sql_exception_list

        print "\n Updated Stock list for - ", len(self.updated_stock_list), " Stocks"
        print self.updated_stock_list

        print "\n qData_missing_stock_list list for - ", len(self.qData_missing_stock_list), " Stocks"
        print self.qData_missing_stock_list


        print("\n\nTime Taken --- in minutes ---" , int((time.time() - start_time))/60 )
        #EmailUtil.send_email("Update Final Rating Exeption List",self.sql_exception_list,  "")
