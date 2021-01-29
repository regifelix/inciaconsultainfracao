import sys
import logging
import pymysql
from src.configuration.rds_config import RdsConfig
from datetime import datetime, timezone, timedelta


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class InfractionRepo:
    def __init__(self, rds_config=None):
        self.__prefix = "[infraction_repository]"
        self.__conn = None
        if rds_config is None:
            self.__rds_config = RdsConfig()
            self.__rds_config.load_config()
        else:
            self.__rds_config = rds_config
        self.__criar_conexao()

    @property
    def rds_config(self):
        return self.__rds_config

    @rds_config.setter
    def rds_config(self, rds_config):
        self.__rds_config = rds_config

    def __criar_conexao(self):
        jdbc_rds_host = self.__rds_config.rds_host
        user = self.__rds_config.db_username
        passwd = self.__rds_config.db_passwd
        db_name = self.__rds_config.db_name
        rds_host = jdbc_rds_host.split("//")[1]
        rds_host = rds_host.replace("jdbc:mysql://", "")
        rds_host = rds_host.replace("/" + db_name, "")
        try:
            self.__conn = pymysql.connect(host=rds_host, user=user,
                                          passwd=passwd, db=db_name,
                                          port=3306)
            logger.info("%sSUCCESS: Connection to RDS MySQL." % self.__prefix)
        except pymysql.MySQLError as e:
            logger.error("%sErro ao conectar ao RDS MySQL." % self.__prefix)
            logger.error(e)
            sys.exit()

    def get(self, ispb):
        try:
            with self.__conn.cursor() as cur:
                cur.execute(f"""select dat_hor_fina_cslt_rlto_infc,
                     cod_ispb_pati_rlto_infc from tbic4008_cslt_rlto_infc_pix
                     where cod_ispb_pati_rlto_infc>= '{ispb}'
                     order by dat_hor_fina_cslt_rlto_infc DESC limit 1""")
                row = cur.fetchone()
                if not row:
                    logger.info(
                        "%sNao encontrada ISPB=[%s] retornado data (D -1)" % (
                            self.__prefix, ispb))
                    dt_last_exec = datetime.now(timezone.utc) - timedelta(1)
                else:
                    dt_last_exec = row[0]

                logger.info("%sRetornado Data=[%s] para ISPB=[%s]" % (
                    self.__prefix,
                    dt_last_exec.isoformat(sep='T', timespec='auto'),
                    ispb
                ))

                return dt_last_exec
        except Exception as error:
            logger.error(f""""{self.__prefix}Erro ao consultar o RDS MySQL.
                         Erro: {error}""")
            raise error
