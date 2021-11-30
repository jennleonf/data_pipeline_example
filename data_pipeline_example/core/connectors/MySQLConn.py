import logging
import mysql.connector

from time import sleep
from data_pipeline_example.core.setting import Settings


class MySqlConnection:
    def __init__(self, table: str):
        self.table = table
        self.client = None

    def connect(self):

        _environment_settings = Settings.environment_settings(
            Settings.get_environment()
        )

        self.mysql_config = _environment_settings["mysql"]

        self.client = mysql.connector.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config["port"],
            user=self.mysql_config["user"],
            password=self.mysql_config["password"],
            database=self.mysql_config["database"],
        )
        self.commit_epochs = self.mysql_config["commit_epochs"]
        logging.info(
            f"MySQL connection done, with epochos of {self.commit_epochs}"
        )
        return self

    def chunk_insert(self, stmt_insert, chunks):
        cur = self.client.cursor()
        try:
            count_items = 0
            count_commit = 0
            count_elemnt = 0

            for i in range(len(chunks)):
                one_slide = [(x[0].item(), x[1].item()) for x in chunks[i]]
                one_slide = [tuple(row) for row in one_slide]
                count_elemnt += len(one_slide)

                cur.executemany(stmt_insert, one_slide)

                count_items += 1
                if count_items == self.commit_epochs or i == len(chunks) - 1:
                    self.client.commit()
                    count_commit += 1
                    count_items = 0

            logging.info(f"Data inserted! {count_commit} times")
            logging.info(f"Amount of items inserted! {count_elemnt}")

        except Exception as e:
            logging.info("Received error:", e)
            self.client.rollback()
            logging.info("db data rollbacked!")
        cur.close()

    def close(self):
        self.client.close()
