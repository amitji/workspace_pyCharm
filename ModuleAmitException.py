# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 06:59:00 2018

@author: amimahes
"""
import sys, os

#class ModuleAmitException:
    
def printInfo():
    #This is to print line # for exception
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
