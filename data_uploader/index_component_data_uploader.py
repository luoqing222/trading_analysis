__author__ = 'Qing'

import MySQLdb
import models
import logging
import os
import datetime

logger = logging.getLogger(__name__)
class IndexComponentDataUploader:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def create_index_component_table(self):
        models.db.init(host=self.host, database=self.database, user=self.user, passwd=self.password)
        models.db.connect()
        if not models.IndexComponentData.table_exists():
            models.db.create_table(models.IndexComponentData)

    def upload_index_component_to_db(self,file_name, folder):
        full_file_name = folder + "/" + file_name
        if not os.path.exists(full_file_name):
            logger.warning(full_file_name + " does not exist!")
            return

        self.create_index_component_table()
        records = []
        with open(full_file_name) as fp:
            next(fp)
            for line in fp:
                line = line.strip()
                splited_item = line.split(',')
                if len(splited_item) == 4:
                    [transaction_date, index, symbol, name] = splited_item
                    transaction_date = datetime.datetime.strptime(transaction_date,"%m/%d/%Y").date().strftime("%Y-%m-%d")
                    try:
                        records.append((transaction_date, index, symbol, name))
                    except:
                        pass

        db = MySQLdb.connect(host=self.host, db=self.database, user=self.user, passwd=self.password)
        cursor = db.cursor()
        sql_statement = "insert into indexcomponentdata(transaction_date, index_symbol, symbol, company_name) values(%s,%s,%s,%s)"
        try:
            cursor.executemany(sql_statement, records)
            db.commit()
            logger.info("%s is successfully uploaded", full_file_name)
        except  Exception, e:
            logger.warning("exception is thrown when upload " + full_file_name + ":" + str(e))
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    def run(self, file_name, folder):
        self.upload_index_component_to_db(file_name, folder)






