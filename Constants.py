
"""
Constants.py
"""

Vendor_Name = 'DEB'
#quater_names = {'2016-06-30': 'Q116', '2016-03-31': 'Q415', '2015-12-31': 'Q315', '2015-09-30': 'Q215',
 #                    '2015-06-30': 'Q115', '2015-03-31': 'Q414', '2014-12-31': 'Q314'}

quarter_dates_v1 = {5: '2016-12-31', 4: '2016-09-30', 3: '2016-06-30', 2: '2016-03-31', 1: '2015-12-31'}
screener_quarter_dates_v1 = {5: 'Dec 2016', 4: 'Sep 2016', 3: 'June 2016', 2: 'Mar 2016', 1: 'Dec 2015'}
quarter_names_v1 = {5: 'Q316', 4: 'Q216', 3: 'Q116', 2: 'Q415', 1: 'Q315'}

quarter_dates = {5: '2017-03-31', 4: '2016-12-31', 3: '2016-09-30', 2: '2016-06-30', 1: '2016-03-31'}
screener_quarter_dates = {5: 'Mar 2017', 4: 'Dec 2016', 3: 'Sep 2016', 2: 'June 2016', 1: 'Mar 2016'}
quarter_names = {5: 'Q117', 4: 'Q316', 3: 'Q216', 2: 'Q116', 1: 'Q415'}

latest_period='2017-03-31'
#below is used in old scripts no longer used
latest_quarter_date = '2016-06-30 00:00:00'
#current_quarter = 'Q415'
previous_quarter = 'Q415'
prev_to_previous_quarter = 'Q315'

Authkey = '5_pRK9pKefuvZzHe-MkSy'


#dataset suffix
revenue_dataset_suffix = '_Q_SR'
profit_dataset_suffix = '_Q_NP'
profit_margin_dataset_suffix = '_Q_NETPCT'
operating_profit_dataset_suffix = '_Q_OP'
operating_prof_margin_dataset_suffix = '_Q_OPMPCT'
ebidt_dataset_suffix = '_Q_EBIDT'
ebidt_margin_dataset_suffix = '_Q_EBIDTPCT'


#Yahoo url & xpaths
yahooUrl_part1='http://finance.yahoo.com/quote/'
yahooUrl_stats='/key-statistics?p='
yahooUrl_financial='/financials?p='

yLastPrice_Xpath='//*[@id="quote-header-info"]/div[2]/div[1]/section/span[1]'
yEPS_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[2]/div[4]/table/tbody/tr[7]/td[2]'
yPE_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[1]/div/table/tbody/tr[3]/td[2]'
yPB_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[1]/div/table/tbody/tr[7]/td[2]'
yROE_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[2]/div[3]/table/tbody/tr[2]/td[2]'
y52WeekHigh_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[2]/div/div[1]/table/tbody/tr[4]/td[2]'
y52WeekLow_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[2]/div/div[1]/table/tbody/tr[5]/td[2]'
yDebt_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[2]/div[5]/table/tbody/tr[3]/td[2]'
yDebtEquityRatio_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[2]/div[5]/table/tbody/tr[4]/td[2]'
yInterest_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[4]/table/tbody/tr[15]/td[2]'
yCurrentRatio_Xpath='//*[@id="main-0-Quote-Proxy"]/section/div[2]/section/div/section/div[2]/div[1]/div[2]/div[5]/table/tbody/tr[5]/td[2]'

#Google url & xpaths
googleUrl = 'https://www.google.com/finance?q=NASDAQ%3A'
googleFinance_Xpath='//*[@id="navmenu"]/li[7]/a'


gRevenueQ5_Xpath='//*[@id="fs-table"]/tbody/tr[1]/td[2]'
gRevenueQ4_Xpath='//*[@id="fs-table"]/tbody/tr[1]/td[3]'
gRevenueQ3_Xpath='//*[@id="fs-table"]/tbody/tr[1]/td[4]'
gRevenueQ2_Xpath='//*[@id="fs-table"]/tbody/tr[1]/td[5]'
gRevenueQ1_Xpath='//*[@id="fs-table"]/tbody/tr[1]/td[6]'

gProfitQ5_Xpath='//*[@id="fs-table"]/tbody/tr[25]/td[2]'
gProfitQ4_Xpath='//*[@id="fs-table"]/tbody/tr[25]/td[3]'
gProfitQ3_Xpath='//*[@id="fs-table"]/tbody/tr[25]/td[4]'
gProfitQ2_Xpath='//*[@id="fs-table"]/tbody/tr[25]/td[5]'
gProfitQ1_Xpath='//*[@id="fs-table"]/tbody/tr[25]/td[6]'

gOPMQ5_Xpath='//*[@id="fs-table"]/tbody/tr[13]/td[2]'
gOPMQ4_Xpath='//*[@id="fs-table"]/tbody/tr[13]/td[3]'
gOPMQ3_Xpath='//*[@id="fs-table"]/tbody/tr[13]/td[4]'
gOPMQ2_Xpath='//*[@id="fs-table"]/tbody/tr[13]/td[5]'
gOPMQ1_Xpath='//*[@id="fs-table"]/tbody/tr[13]/td[6]'

gEBITQ5_Xpath='//*[@id="fs-table"]/tbody/tr[17]/td[2]'
gEBITQ4_Xpath='//*[@id="fs-table"]/tbody/tr[17]/td[3]'
gEBITQ3_Xpath='//*[@id="fs-table"]/tbody/tr[17]/td[4]'
gEBITQ2_Xpath='//*[@id="fs-table"]/tbody/tr[17]/td[5]'
gEBITQ1_Xpath='//*[@id="fs-table"]/tbody/tr[17]/td[6]'


#Screener.in url & xpaths
screenerBaseUrl = 'https://www.screener.in/company/'
screenerBaseUrl_part2='/consolidated/'


# Final Rating Weightages
# revenueW = 1.0
roeW = 0.25
icW = 0.25  # Interest Coverage ratio
deW = 0.25 #debt equity ratio
opmANDebitW = 0.5
genericW = 1.0