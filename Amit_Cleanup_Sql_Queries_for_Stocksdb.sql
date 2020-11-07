#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 15:22:15 2020

@author: amimahes
"""


SELECT * FROM stocksdb.stock_market_data where nseid='3IINFOTECH' and date(last_modified)="2020-07-13"
order by last_modified desc;

DELETE FROM stocksdb.stock_market_data where  date(last_modified)="2020-07-13";


