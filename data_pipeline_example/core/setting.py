import os
import logging

from typing import Dict
from pathlib import Path
from copy import deepcopy


class Settings:

    SETTINGS = {
        "common": {
            "data_path": Path("data_pipeline_example/data")
        },
        "environment": {
            "prod": {
                "mysql": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "user",
                    "password": "password",
                    "database": "warehouse",
                    "chunk_size": 2000,
                    "commit_epochs": 1000,
                }
            },
            "test": {
                "mysql": {
                    "host": "172.18.0.2",
                    "port": 3306,
                    "user": "user",
                    "password": "password",
                    "database": "warehouse",
                    "chunk_size": 2000,
                    "commit_epochs": 1000,
                }
            }
        },
        "etl": {
            "json_loader": {
                "table_name": "tacks",
                "file_name": "202106_flink_data_engieering_sample_data.json",
                "schema": {
                    "type": "object",
                    "properties": {
                        'event_type': {'type': 'integer'},
                        'event_time': {'type': 'string', 'format': 'date-time'},
                        'data': {'user_email': {'type': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'},
                                 'phone_number': {'type': r'^\d{10}$'}},
                        'processing_date': {'type': 'string', 'format': 'date'}
                    },
                }
            }
        }
    }

    @classmethod
    def get_environment(cls):
        environment = "test"
        env_param = os.getenv("ENV")
        if env_param is not None:
            tmp_env = (env_param).lower()[:4]
            if tmp_env in cls.SETTINGS["environment"].keys():
                environment = tmp_env
        return environment

    @classmethod
    def environment_settings(cls, environment: str) -> Dict:
        return cls.SETTINGS["environment"][environment]

    @classmethod
    def _get_basic_configuration(cls) -> dict:
        """
        Returns:
            dict: common and environment configs
        """
        configuration = {}
        environment = cls.get_environment()
        configuration["env"] = environment
        configuration.update(cls.environment_settings(environment))

        # data_path/environment
        configuration.update(cls.SETTINGS["common"])
        configuration["data_path"] = configuration["data_path"] / environment

        return deepcopy(configuration)

    @classmethod
    def etl_settings(cls):
        configuration = cls._get_basic_configuration()
        configuration.update(cls.SETTINGS["etl"])
        logging.info("ETL settings loaded for %s environment",
                     configuration.get("env"))
        return configuration
