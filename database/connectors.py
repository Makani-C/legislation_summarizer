import logging
import os
from configparser import ConfigParser

from database.connection_models import MariaDB, PostgresDB

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_database_configuration():
    file_directory = os.path.dirname(os.path.realpath(__file__))
    config = ConfigParser()
    config.read(f"{file_directory}/config.ini")

    return config


class RDSConnection(PostgresDB):
    def __init__(self):
        # Read the database credentials from config.ini
        config = get_database_configuration()
        super().__init__(
            host=config.get("rds", "rds_host"),
            port=config.getint("rds", "rds_port"),
            user=config.get("rds", "rds_user"),
            password=config.get("rds", "rds_password"),
            database=config.get("rds", "rds_database"),
        )


class LegiscanDBConnection(MariaDB):
    def __init__(self):
        pass
