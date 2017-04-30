#import scrapping
import csv
from custom_logging import logger
from collections import OrderedDict
import win32com.client
import env

def decorator(func):
    def create_dict(data, csvfile):
        raw_data = data.splitlines()
        temp = map(lambda x: x.split(":"), raw_data)
        logger.info (temp)
        processed_data = OrderedDict(temp)
        logger.info(processed_data)
        logger.info("writing in csv")
        func(processed_data, csvfile)
    return create_dict 

@decorator
def write_to_csv(data_in_dictionary, csvfile):
    """Write data to a csv file"""
    with open(csvfile, 'wb') as f:
        fieldnames = data_in_dictionary.keys()
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        writer.writerow(data_in_dictionary)
        logger.info("Data written to file: " + csvfile)

def read_from_xlsx(filename='Infosys.xlsx', sheet='Profit & Loss', cell_range=OrderedDict([('A14','J14'), ('A15','J15')])):
    """To read from excel downloaded"""
    xl = win32com.client.Dispatch('Excel.Application')
    try:
        filename = env.DOWNLOAD_DIR + '\\' + filename
        wb = xl.Workbooks.Open(Filename=filename, ReadOnly=1, Editable=True)
        ws = wb.Worksheets(sheet)
        for k, val in cell_range.items():
            print (ws.Range(k + ':' + val).Value)

    except Exception as e:
        logger.exception(e)

    else:
        wb.Close(True)
