# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 18:46:11 2018

@author: amahe6
"""

from urllib.request import urlopen
import requests
import DBManager
import json

class Module_Get_Live_Data_From_Google:
    def __init__(self):
        self.con = DBManager.connectDB()
        self.cur = self.con.cursor()

        self.url_prefix = "https://finance.google.com/finance?q="
        self.url_suffix = "&output=json"
        
        
    def getLiveQuotesForAStock(self, nseid):

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

    def getLiveQuotesForMultipleStock(self, stock_names):

        try:
            content = []


            for row in stock_names:
                try:
                    fullid = 'NSE:'+row['nseid']
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
                    continue

                #print content2
                content.append(content2);

            #print content

        except Exception as e:
            print ("\n******Amit exception in getAllQuotes \n ")
            print (str(e))
            pass


        return content

