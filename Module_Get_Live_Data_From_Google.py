# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 18:46:11 2018

@author: amahe6
"""

from urllib.request import urlopen
import requests
import DBManager
import json
import ModuleAmitException
import NSELiveDataModule


class Module_Get_Live_Data_From_Google:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.url_prefix = "https://finance.google.com/finance?q="
        self.url_suffix = "&output=json"
        
    '''       
    def getLiveQuotesForAStock_OldV1(self, nseid):

        try:
            fullid = 'NSE:'+nseid
            if '&' in fullid:
                fullid = fullid.replace('&','%26')
            url = self.url_prefix+ "%s" % ( fullid)+self.url_suffix
            #print "\nurl - ", url
            rsp = requests.get(url)
            content2 = {}
            if rsp.status_code in (200,):
                temp = rsp.content
                #print temp
                fin_data = json.loads(rsp.content[6:-2].decode('unicode_escape'))
                #print fin_data

                #print('Last Trade Price: {}'.format(fin_data['l']))
                #print('Change: {}'.format(fin_data['c']))
                #print('Change %: {}'.format(fin_data['cp']))


                lp = fin_data['l'];
                volume = fin_data['vo'];

                if 'M' in volume:
                    volume = volume.replace("M", "")
                    volume = float((volume).replace(",", ""))
                    volume = volume * 1000000  ## convert million into number
                elif 'B' in volume:    
                    volume = volume.replace("B", "")
                    volume = float((volume).replace(",", ""))
                    volume = volume * 1000000000  ## convert million into number
                    
                else:
                    volume = float((volume).replace(",", ""))

                content2['fullid'] = fullid;
                lp = float(('{}'.format(fin_data['l'])).replace(",", ""));
                change = float(('{}'.format(fin_data['c'])).replace(",", ""));

                prev_close = lp-change;

                content2['l'] = '{}'.format(fin_data['l']).replace(",", "");
                content2['c'] = '{}'.format(fin_data['c']).replace(",", "");
                content2['cp'] = '{}'.format(fin_data['cp']).replace(",", "");
                content2['pcls'] = '{}'.format(prev_close);

                content2['e'] = '{}'.format(fin_data['e']);
                content2['t'] = '{}'.format(fin_data['t']);
                content2['volume'] = '{}'.format(volume);
        except Exception as e1:
            print ("\n******Amit exception in getAllQuotes for fullid - \n ", fullid)
            print (str(e1))
            
            

        return content2

    def getLiveQuotesForMultipleStock_OldV1(self, stock_names):

        try:
            content = []


#            for row in stock_names:
            for i, row in stock_names.iterrows():
                try:
                    nseid = row['nseid']
                    fullid = 'NSE:'+nseid
                    if '&' in fullid:
                        fullid = fullid.replace('&','%26')
                    url = self.url_prefix+ "%s" % ( fullid)+self.url_suffix
                    #print "\nurl - ", url
                    rsp = requests.get(url)
                    content2 = {}
                    if rsp.status_code in (200,):
                        temp = rsp.content
                        #print temp
                        fin_data = json.loads(rsp.content[6:-2].decode('unicode_escape'))
                        #print fin_data

                        #print('Last Trade Price: {}'.format(fin_data['l']))
                        #print('Change: {}'.format(fin_data['c']))
                        #print('Change %: {}'.format(fin_data['cp']))


                        lp = fin_data['l'];
                        volume = fin_data['vo'];

                        if 'M' in volume:
                            volume = volume.replace("M", "")
                            volume = float((volume).replace(",", ""))
                            volume = volume * 1000000  ## convert million into number
                        elif 'B' in volume:    
                            volume = volume.replace("B", "")
                            volume = float((volume).replace(",", ""))
                            volume = volume * 1000000000  ## convert million into number
                            
                        else:
                            volume = float((volume).replace(",", ""))
                        volume = round(volume)        
                        content2['fullid'] = fullid;
                        content2['nseid'] = nseid;
                        lp = float(('{}'.format(fin_data['l'])).replace(",", ""));
                        change = float(('{}'.format(fin_data['c'])).replace(",", ""));

                        prev_close = lp-change;

                        content2['l'] = '{}'.format(fin_data['l']).replace(",", "");
                        content2['c'] = '{}'.format(fin_data['c']).replace(",", "");
                        content2['cp'] = '{}'.format(fin_data['cp']).replace(",", "");
                        content2['pcls'] = '{}'.format(prev_close);

                        content2['e'] = '{}'.format(fin_data['e']);
                        content2['t'] = '{}'.format(fin_data['t']);
                        content2['volume'] = '{}'.format(volume);
                except Exception as e1:
                    print ("\n******Amit exception in getLiveQuotesForMultipleStock for fullid - \n ", fullid)
                    print (str(e1))
                    continue

                #print content2
                content.append(content2);

            #print content

        except Exception as e:
            print ("\n******Amit exception in getLiveQuotesForMultipleStock \n ")
            print (str(e))
            pass


        return content
    '''
  
    def getLiveQuotesForMultipleStock(self, stock_names,quandlData):
        
        url_prefix = "https://finance.google.com/finance/getprices?q="
#        url_suffix = "&x=NSE&i=60&p=1d&f=d,c,o,h,l"
        url_suffix_nse = "&x=NSE&i=60"
        url_suffix_bom = "&x=BOM&i=60"

        try:
            content = []
            for row in stock_names:
                try:
                    fullid = row['fullid']
                    nseid = row['nseid']
                    if '&' in nseid:
                        nseid = nseid.replace('&','%26')
                        
                    if "BOM:" in fullid:
                        url = url_prefix+ "%s" % ( nseid)+url_suffix_bom
                    else:
                        url = url_prefix+ "%s" % ( nseid)+url_suffix_nse   
                        
                    rsp = requests.get(url,headers={'User-Agent': 'Mozilla/5.0'})
                    content2 = {}
                    if rsp.status_code in (200,):
                        temp = rsp.content
                        last_line = temp.splitlines()[-1]
                        fin_data = last_line.decode("utf-8").split(',')
                        qrow = quandlData.loc[quandlData['fullid'] == fullid]
                        lp = float(fin_data[1])
                        volume = float(fin_data[5])                        
                        prev_close = float(qrow['pcls'].iloc[0])

                        content2['fullid'] = fullid
                        change = lp-prev_close
                        change_percent = float((change * 100 )/prev_close)
                        change_percent = "{0:.2f}".format(change_percent)

                        content2['l'] = '{}'.format(lp).replace(",", "");
                        content2['c'] = '{}'.format(change).replace(",", "");
                        content2['cp'] = '{}'.format(change_percent).replace(",", "");
                        content2['pcls'] = '{}'.format(prev_close);
                        content2['volume'] = '{}'.format(volume);
                except Exception as e1:
                    print ("\n******Amit exception in getAllQuotes for fullid - \n ", fullid)
                    print (str(e1))
                    ModuleAmitException.printInfo()
                    pass
                content.append(content2);
        except Exception as e:
            print ("\n******Amit exception in getAllQuotes \n ")
            print (str(e))
            ModuleAmitException.printInfo()
            pass
        return content

    def getLiveQuotesForAStock(self, nseid):
        
        url_prefix = "https://finance.google.com/finance/getprices?q="
#        url_suffix = "&x=NSE&i=60&p=1d&f=d,c,o,h,l"
        url_suffix = "&x=NSE&i=60"
        fullid = "NSE:"+nseid  # this assume its a NSE stock
        quandlData = self.getAllQuotesFromQuandl([{'nseid':nseid, 'fullid':fullid}])            

        try:
            
            
            if '&' in nseid:
                nseid = nseid.replace('&','%26')
            url = url_prefix+ "%s" % ( nseid)+url_suffix
            
            rsp = requests.get(url,headers={'User-Agent': 'Mozilla/5.0'})
            content2 = {}
            if rsp.status_code in (200,):
                temp = rsp.content
                last_line = temp.splitlines()[-1]
                fin_data = last_line.decode("utf-8").split(',')
#                    qrow = quandlData.loc[quandlData['fullid'] == fullid]
                qrow  = quandlData[0]  # Here its only one stocks so get with 0 index
                lp = float(fin_data[1])
                volume = float(fin_data[5])
                prev_close = float(qrow['pcls'])

                content2['fullid'] = fullid
                change = lp-prev_close
                change_percent = float((change * 100 )/prev_close)
                change_percent = "{0:.2f}".format(change_percent)

                content2['l'] = '{}'.format(lp).replace(",", "");
                content2['c'] = '{}'.format(change).replace(",", "");
                content2['cp'] = '{}'.format(change_percent).replace(",", "");
                content2['pcls'] = '{}'.format(prev_close);
                content2['volume'] = '{}'.format(volume);
        except Exception as e1:
            print ("\n******Amit exception in getAllQuotes for fullid - \n ", fullid)
            print (str(e1))
            ModuleAmitException.printInfo()
            pass

        return content2
    
    def getAllQuotesFromQuandl(self, stock_names):

        try:
            content = []


            for row in stock_names:
                try:
                    content2 = {}
                    fullid = row['fullid']
                    nseid = row['nseid']
#                    nseid = 'NCC'
                    mydata = NSELiveDataModule.getQuandlData(fullid,nseid)
                    fin_data = NSELiveDataModule.getLastDayParams(mydata,fullid,nseid)
                    
#                    print(fin_data)
            
    
                    content2['fullid'] = fullid

    
                    content2['l'] = '{}'.format(fin_data['close'][0]).replace(",", "");
                    content2['c'] = '{}'.format(fin_data['change'][0]).replace(",", "");
                    content2['cp'] = '{}'.format(fin_data['percent_change'][0]).replace(",", "");
                    content2['pcls'] = '{}'.format(fin_data['prev_close'][0]).replace(",", "");
                    content2['volume'] = '{}'.format(fin_data['volume'][0]).replace(",", "");
#
#    
#                    content2['l'] = fin_data['close'][0]
#                    content2['c'] = fin_data['change'][0]
#                    content2['cp'] = fin_data['prev_close'][0]
#                    content2['pcls'] = fin_data['prev_close'][0]
#                    content2['volume'] = fin_data['volume'][0]


                except Exception as e1:
                    print ("\n******Amit exception in getAllQuotes for fullid - \n ", fullid)
                    print (str(e1))
                    ModuleAmitException.printInfo()
                    pass
    
                print (content2)
                content.append(content2);

            #print content

        except Exception as e:
            print ("\n******Amit exception in getAllQuotes \n ")
            print (str(e))
            ModuleAmitException.printInfo()
            pass


        return content    