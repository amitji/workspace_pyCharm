import requests, re
#from pushbullet import Pushbullet
import pandas as pd
import pickle, time

##List of Stocks we want to Screen( choose from nifty50,nifty100,nifty200,nifty500)
#with open('nifty100.pkl','rb') as f:
#    nifty100 = pickle.load(f)
#
#seclist = nifty100['NCC'].values
seclist = ['530537']
##To Avoid Duplicates
#ofile = open('Duplicates.txt','a+')
#dupfile = open('Duplicates.txt','r').readlines()
#duplicates = [s.rstrip() for s in dupfile]


##Pushbullet API Key
#pb = Pushbullet('o.uvaVlngkt98QbvUZap8Y2kg5lJaSDHzx')

###Scraping the Website

url = "http://www.bseindia.com/corporates/ann.aspx"
r = requests.get(url).text
stockannouncements = re.findall("<tr><td class='TTHeadergrey'(.*?[^:]..)</td></tr><tr><td class='TTRow_leftnotices' colspan ='4'>",r)

###Looking for Our Stocks of Interest in the Announcements

for info in stockannouncements:
    seccode = re.findall("- (\d{6}) -",info)
    loc = [i for i,x in enumerate(seclist) if x == int(seccode[0])]
    if len(loc)>0:
        headline = re.findall(">([A-Z].*?)</td><td class='TTHeadergrey'",info)
        pdflink = re.findall("valign='middle'><a class='tablebluelink' href = '(.*?)' target = '_blank'>",info)

        ##Structuring Head & Body of Notification
        if len(headline)>0 and headline[0].replace('</a>','') not in duplicates:
            body = re.findall("\d{2}</td></tr><tr><td class='TTRow_leftnotices' colspan ='4'>(.*?$)",info)
            try:
                head = headline[0].replace('</a>','')+'\n'
                bodymessage = (body[0]+'\n\nLink to attachment:\n'+pdflink[0]+'\n')
                ofile.write(head)
                print (head,bodymessage) ##Use to troubleshoot
                ##Pushing the Notification
#                push = pb.push_note(head,bodymessage)
            except IndexError:
                pass
            time.sleep(2)

ofile.close()
