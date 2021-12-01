import math
import json
import logging
import jsonschema
import numpy as np

from pathlib import Path
from typing import Tuple
from data_pipeline_example.core.setting import Settings
from data_pipeline_example.core.connectors.MySQLConn import MySqlConnection
from jsonschema import validate

logging.basicConfig(level=logging.DEBUG)

logging.debug('This will get logged')


class ETLJsonLoader:
    """
    Class in charge of reading, validate and save into MySQL, data from json file

    Attributes
    ----------

    Methods
    -------
    read():
        Function in charge of reading the json file line by line and validating if each line complies with the scheme
    _validate():
        Function in charge of validate if a given json has the correct schema
    save():
        Function in charge of save the data into Mysql
    run():
        Function in charge of sequentially performing the ETL

    _get_data_to_insert():
        Function is in charge of transforming the data for insertion
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
                if self._validate(tmp_line):
                    self.data.append(tmp_line)
        logging.info(f'Amount of valid rows %s', len(self.data))

        for i in range(len(self.data)):
            for key, value in self.data[i].items():
                if key == 'data':
                    self.data[i][key] = json.dumps(value)

    def _validate(self, item):
        try:
            validate(instance=item, schema=self.schema)
        except jsonschema.exceptions.ValidationError as err:
            logging.info(err)
            return False
        return True

    def save(self):

        data_to_insert = self._get_data_to_insert(self.data)
        stmt_insert = (
            f"INSERT INTO {self.table_name.lower()} (event_type, event_time, data, processing_date) VALUES (%s, %s, %s, %s)"
        )
        self.mysql_connector.chunk_insert(stmt_insert, data_to_insert)
        self.mysql_connector.close()

    def run(self):
        logging.info('Running JSON loader')
        self.read()
        logging.info('Saving valid data')
        self.save()

    @staticmethod
    def _get_data_to_insert(data_to_insert):
        _environment_settings = Settings.environment_settings(
            Settings.get_environment()
        )
        chunk_size = _environment_settings["mysql"]["chunk_size"]

        size = math.ceil(len(data_to_insert) / chunk_size)
        data_to_insert = [item.values() for item in data_to_insert]
        data_chunks = np.array_split(data_to_insert, size)
        del data_to_insert
        return data_chunks
