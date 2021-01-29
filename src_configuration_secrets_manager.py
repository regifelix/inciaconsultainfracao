import boto3
import base64
import logging
from botocore.exceptions import ClientError


class SecretsManager:
    def __init__(self):
        region_name = "sa-east-1"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        self.__client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

    def get_secret(self, client, secret_arn):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        secret_name_with_suffix = secret_arn.split(':')[6]
        if secret_name_with_suffix is None:
            logger.error("secret_name_with_suffix está vindo nulo")
            raise RuntimeError
        split_elements = secret_name_with_suffix.split('-')
        secret_name = split_elements[0] + "-" + split_elements[1]

        # In this sample we only handle the specific exceptions
        # for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/
        # apireference/API_GetSecretValue.html
        # We rethrow the exception by default.
        InternalServiceError = 'InternalServiceErrorException'
        try:
            secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            logging.error("ERROR:{}".format(e))
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected
                # secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow
                # at your discretion.
                raise e
            elif e.response['Error']['Code'] == InternalServiceError:
                # An error occurred on the server side.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or
                # rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not
                # valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow
                # at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow
                # at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or
            # binary, one of these fields will be populated.
            if 'SecretString' in secret_value_response:
                return secret_value_response['SecretString']
            else:
                return base64.b64decode(secret_value_response['SecretBinary'])

    @property
    def client(self):
        return self.__client
