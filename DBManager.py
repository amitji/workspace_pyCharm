'''
Created on Nov 30, 2015

@author: Amit Maheshwari
'''
#!/usr/bin/python
import mysql.connector
from nsetools import Nse
from sqlalchemy import create_engine

def createEngine():
    return create_engine('mysql+mysqlconnector://shop_dba:shop2015@103.35.123.14:3306/stocksdb', echo=False)
    
def connectDB():
    # connect
    db = mysql.connector.connect(host="103.35.123.14", user="shop_dba", passwd="shop2015",db="stocksdb")
    #db = mysql.connector.connect(host="localhost", user="shop_dba", passwd="shop2015", db="stocksdb")

    #cursor = db.cursor()
    return db

def disconnectDB():
    print ("Empty disconnect")
# execute SQL select statement

# commit your changes
#db.commit()

# get the number of rows in the resultset
#numrows = int(cursor.rowcount)

# get and display one row at a time.
#for x in range(0,numrows):
 #   row = cursor.fetchone()
  #  print row[0], "-->", row[1]

    