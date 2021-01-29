import logging
import json
import time
from src.tools.queue_tools import QueueTools
from src.configuration.sqs_config import SqsConfig
from src.repository import infraction_repository
from src.tools.helpers import integration_tests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# messages
prefix = "[lambda_function]"
return_error = "Lambda handler execution finished with error." \
               " See application logs for more details."
# parametros
ispb_itau = "60701190"

# Lambda sera executada N vezes por minuto de forma a ter execucoes
total_execucoes = 3
pausa_segundos = 20


# Handler (Main)
# @integration_tests(bypass=True)
def lambda_handler(event, context):
    sqs_config = SqsConfig()
    sqs_config.load_sqs_queue()
    queue_name = sqs_config.queue_name

    infraction_repo = infraction_repository.InfractionRepo()
    queue_tools = QueueTools()
    queue = queue_tools.get_queue(queue_name)
    cont = 1
    while cont < (total_execucoes + 1):
        logger.info("%s[%s of %s]Iniciando execucao." % (
            prefix, str(cont), str(total_execucoes)))
        dt_ultima_execucao = infraction_repo.get(ispb_itau)

        msg_content = get_message_body(dt_ultima_execucao)
        send_to_queue(queue_name, queue, queue_tools, msg_content, cont)

        cont = cont + 1
        time.sleep(pausa_segundos)

    return "Finalizado execução da lambda."


def get_message_body(dt_ultima_execucao):
    msg_content = json.dumps(
        dict(
            date=dt_ultima_execucao.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            ispb=ispb_itau)
    )
    return msg_content


def send_to_queue(queue_name, queue, queue_tools, msg_content, cont):
    logger.info(
        "%s[%s of %s]Enviando para fila [%s] msg=[%s]" % (
            prefix, str(cont), str(total_execucoes),
            queue_name, msg_content)
    )
    response = queue_tools.send_message(queue, msg_content)

    logger.info("%s[%s of %s]Finalizado envio fila [%s] "
                "msg=[%s] correlation_id=[%s]." % (
                    prefix,
                    str(cont),
                    str(total_execucoes),
                    queue_name,
                    msg_content,
                    response['MessageId']))
