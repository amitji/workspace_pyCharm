import MySQLdb
import env
from custom_logging import logger

class DBHandler:
    db = ''
    cursor = ''
    def __init__(self, query = 'use python;'):
        try:
            DBHandler.db = MySQLdb.connect(env.DB_IP, env.DB_USER, env.DB_PASSWORD, env.DB_DATABASE)
            logger.info('Connection created !!')
            DBHandler.cursor = DBHandler.db.cursor()
            self.query = query
            logger.debug('Running query: ' + self.query)
            DBHandler.cursor.execute(self.query)
        except:
            logger.exception('Connecetion Failed')

    def __enter__(self):
        logger.debug('############################################')
        return self

    def insert(self, query):
        self.query = query
        try:
            logger.debug('Running query: ' + self.query)
            DBHandler.cursor.execute(self.query)
            DBHandler.db.commit()
            logger.info('Query executed')
        except:
            logger.exception('Query Failed')
            DBHandler.db.rollback()

    def execute(self, query):
        self.query = query
        try:
            logger.debug('Running query: ' + self.query)
            DBHandler.cursor.execute(self.query)
            result = DBHandler.cursor.fetchall()
            return result
        except:
            result = 'Query Failed'
            logger.exception('Query Failed')
            return result

    def __exit__(self, exc_type, exc_value, traceback):
        DBHandler.db.close()
        logger.info('******************************************')
        logger.info('Destroying DB object')
        logger.info('******************************************')

