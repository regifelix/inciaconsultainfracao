import logging
import boto3
import traceback


class SqsConfig:
    def __init__(self):
        self.__queue_name = ""
        self.__queue_ssm_path = '/config/dict/application/' \
                                'queue_dict_consultainfracao_bacen'

    def get_queue_name_sufix(self, queue_arn):
        if queue_arn is None:
            return ""
        list_split = queue_arn.split('/')
        if list_split.__len__() > 0:
            name_suffix = list_split[list_split.__len__() - 1]
        else:
            name_suffix = queue_arn
        return name_suffix

    def load_sqs_queue(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        client = boto3.client('ssm')
        path_name = self.__queue_ssm_path
        queue_arn = SqsConfig.load_param_store_value(client, path_name)
        self.__queue_name = self.get_queue_name_sufix(queue_arn)

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
                print("Parameter Value: " + str(param_value))
                return str(param_value)
        except Exception:
            print("Encountered an error loading config from SSM.")
            traceback.print_exc()

    @property
    def queue_name(self):
        return self.__queue_name

    @property
    def queue_ssm_path(self):
        return self.__queue_ssm_path
