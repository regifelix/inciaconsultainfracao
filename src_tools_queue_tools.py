import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()


class QueueTools:
    def __init__(self, sqs_config=None):
        self.__sqs = None
        if sqs_config is None:
            self.__sqs = boto3.resource('sqs')
        else:
            self.__sqs = sqs_config

    def get_queue(self, name):
        try:
            logger.info(self.__sqs)
            queue = self.__sqs.get_queue_by_name(QueueName=name)
            logger.info("[queue_tools] Got queue '%s'"
                        " with URL=%s", name, queue.url)
        except ClientError as error:
            logger.exception("[queue_tools] Couldn't get queue named %s.",
                             name)
            raise error
        else:
            return queue

    def send_message(self, queue, message_body, message_attributes=None):
        if not message_attributes:
            message_attributes = {}
        try:
            response = queue.send_message(
                MessageBody=message_body,
                MessageAttributes=message_attributes
            )
        except ClientError as error:
            logger.exception("[queue_tools] ERROR:"
                             " Send message failed: %s", message_body)
            raise error
        else:
            logger.debug("[queue_tools] Message sent successfully")
            return response

    @property
    def sqs(self):
        return self.__sqs
