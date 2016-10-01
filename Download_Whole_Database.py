import quandl

Authkey = '5_pRK9pKefuvZzHe-MkS'
try:
    quandl.bulkdownload('DEB',api_key=Authkey, filename='E:\\001_Stock_Market_Analysis\\Quandl\\DEB Whole Database\\DEB_DB_01_Sep_2016.zip')
except Exception as e:
    print (str(e))

print ("Download done !")