#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 17:19:07 2021

@author: amimahes
"""

import numpy as np
import pandas as pd
import DBManager


class Process_Insert_CSV_into_Table:
    
   def __init__(self):
        self.all_good_flag = True
        self.con = DBManager.connectDB()
        self.engine = DBManager.createEngine()
        self.cur = self.con.cursor()

   def saveInDB(self,file_path,table_name):
           
       df = pd.read_csv(file_path) 
       df.to_sql(table_name, self.engine, if_exists='append', index=False)
       print("Data saved in DB")         


if __name__ == "__main__":
    thisObj = Process_Insert_CSV_into_Table()
    file_path="./nse_stocks_list.csv"
    table_name = "stock_names_new"
    thisObj.saveInDB(file_path,table_name)
    
