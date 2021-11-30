import json
import logging
import jsonschema

from pathlib import Path
from typing import Tuple
from data_pipeline_example.core.setting import Settings
from data_pipeline_example.core.connectors.MySQLConn import MySqlConnection
from jsonschema import validate, FormatChecker
logging.basicConfig(level=logging.DEBUG)
logging.debug('This will get logged')


class ETLJsonLoader:
    """
    Class in charge of reading, validate and save into MySQL, data from json file

    Attributes
    ----------
     mysql_connector
        This attribute is the instance for MySQL connector
    Methods
    -------
    read():
        Function in charge of reading the json file line by line and validating if each line complies with the scheme
    validate():
        Function in charge of validate if a given json has the correct schema
    save():
        Function in charge of save the data into Mysql
    run():
        Function in charge of sequentially performing the ETL

    Use
    ------
    json_loader = ETLJsonLoader()
    json_loader.run()
    """

    def __init__(self):

        default_settings = Settings.etl_settings()
        self.json_loader_settings = default_settings['json_loader']

        self.schema = self.json_loader_settings['schema']
        self.table_name = self.json_loader_settings['table_name']

        self.file_name = default_settings["data_path"] / \
            self.json_loader_settings['file_name']
        self.mysql_connector = MySqlConnection(table=self.table_name).connect()
        self.data = []

    def read(self):
        logging.info(f'reading the file %s', self.file_name)
        with open(self.file_name, 'r') as f:
            for line in f:
                tmp_line = json.loads(line)
                if self.validate(tmp_line):
                    self.data.append(tmp_line)

        logging.info(f'Amount of valid rows %s', len(self.data))

    def validate(self, item):
        try:
            validate(instance=item, schema=self.schema)
        except jsonschema.exceptions.ValidationError as err:
            logging.info(err)
            return False
        return True

    def save(self):
        stmt_insert = (
            f"INSERT INTO {self.table_name.lower()} (user_id, budget) VALUES (%s, %s)"
        )
        self.mysql_connector.chunk_insert(stmt_insert, data)
        self.mysql_connector.close()

    def run(self):
        logging.info('Running JSON loader')
        self.read()
