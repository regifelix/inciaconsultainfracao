import boto3
import traceback
import logging
from src.configuration.secrets_manager import SecretsManager


class RdsConfig:
    def __init__(self):
        self.__rds_host = "//"
        self.__db_username = ""
        self.__db_passwd = None
        self.__db_name = ""

    def load_config(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        client = boto3.client('ssm')
        rds_enpoint_config = "/rds/relatainfracaords-RDSEndpoint"
        db_username_config = "/rds/relatainfracaords-RDSInstanceUserSecret"
        db_name_config_path = "/rds/relatainfracaords-RDSName"
        self.__rds_host = RdsConfig. \
            load_param_store_value_and_secrets_manager(client,
                                                       rds_enpoint_config)
        self.__db_username = RdsConfig. \
            load_param_store_value_and_secrets_manager(client,
                                                       db_username_config)
        self.__db_passwd = RdsConfig. \
            load_param_store_value_and_secrets_manager(client,
                                                       "/rds/relatainfracaords"
                                                       "-RDSInstancePassword"
                                                       "Secret"
                                                       )
        self.__db_name = RdsConfig. \
            load_param_store_value(client, db_name_config_path)

    @staticmethod
    def load_param_store_value_and_secrets_manager(client, param_config_path):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        try:
            # Get all parameters for this app
            param_details = client.get_parameter(
                Name=param_config_path,
                WithDecryption=False
            )
            # Loop through the returned parameters
            # and populate the ConfigParser
            if param_details.get('Parameter') is not None:
                param_value = param_details['Parameter']['Value']
                logger.debug("Parameter Value: " + str(param_value))
                secret_manager = SecretsManager()
                return secret_manager.get_secret(
                    secret_manager.client,
                    param_value)
        except Exception:
            print("Encountered an error loading config from SSM.")
            traceback.print_exc()

    @staticmethod
    def load_param_store_value(client, param_config_path):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        try:
            # Get all parameters for this app
            param_details = client.get_parameter(
                Name=param_config_path,
                WithDecryption=False
            )
            # Loop through the returned parameters
            # and populate the ConfigParser
            if param_details.get('Parameter') is not None:
                param_value = param_details['Parameter']['Value']
                logger.debug("Parameter Value: " + str(param_value))
                return param_value
        except Exception:
            print("Encountered an error loading config from SSM.")
            traceback.print_exc()

    @property
    def rds_host(self):
        return self.__rds_host

    @property
    def db_username(self):
        return self.__db_username

    @property
    def db_passwd(self):
        return self.__db_passwd

    @property
    def db_name(self):
        return self.__db_name
