from scrapping import scraping
from custom_logging import logger
from dbHandler import DBHandler
import dataProcessing
from collections import OrderedDict

def main():
    logger.info("Execeution Begins")
    logger.info("******************************")

    with scraping() as sc:
        sc.export_to_excel(email='d.aggarwal07@yahoo.com', password='dishu@07')
        #sc.general_data(start_word='Market', end_word_not_including='List')

    #dataProcessing.read_from_xlsx()#cell_range=OrderedDict([('A1','C1'),('A2','C2'),('A3','C3')]))

    #with DBHandler() as db:
        #logger.info("Created DB Object")
        #db.insert("load data local infile 'data.csv' into table webdata fields terminated by ',' optionally enclosed by '\"' ignore 1 lines;")
        #result = db.execute('select * from webdata;')
        #logger.info(result)

if __name__ == '__main__':
    main()
    
