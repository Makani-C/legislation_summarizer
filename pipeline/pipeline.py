import io
import sys
import os
import logging
import requests
import traceback

from collections import namedtuple
from datetime import datetime
from configparser import ConfigParser
from PyPDF2 import PdfReader
from sqlalchemy import func, inspect
from sqlalchemy.exc import IntegrityError

filepath = os.path.realpath(__file__)
root_dir = os.path.dirname(os.path.dirname(filepath))
sys.path.append(root_dir)

from database import connection, orm

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Read configuration file
config = ConfigParser()
config.read(f"{root_dir}/config.ini")

# Read database credentials
maria_host = config.get("maria_db", "maria_host")
maria_user = config.get("maria_db", "maria_user")
maria_password = config.get("maria_db", "maria_password")
maria_database = config.get("maria_db", "maria_database")

# Read RDS credentials
rds_host = config.get("rds", "rds_host")
rds_port = config.get("rds", "rds_port")
rds_user = config.get("rds", "rds_user")
rds_password = config.get("rds", "rds_password")
rds_database = config.get("rds", "rds_database")

# Initialize MariaDB and RDS instances
maria_db = connection.MariaDB(
    host=maria_host,
    user=maria_user,
    password=maria_password,
    database=maria_database
)
rds_db = connection.PostgresDB(
    host=rds_host,
    port=rds_port,
    user=rds_user,
    password=rds_password,
    database=rds_database
)


def get_last_pull_timestamp(model: orm.Base):
    """ Get the timestamp of the last pull from the RDS table.

    Args:
        model (orm.Base): RDS table to get timestamp for

    Returns:
        datetime.datetime: The last pull timestamp.
    """
    rds_db.connect()
    try:
        latest_updated_date = rds_db.session.query(func.max(model.updated_at)).scalar()
    finally:
        rds_db.close_connection()

    return latest_updated_date or datetime.min


def get_updated_data(source_query: str, last_pull_timestamp: datetime = None):
    """
    Get the updated data from the MariaDB table.

    Args:
        last_pull_timestamp (datetime.datetime): The timestamp of the last pull.
        table (str): MariaDB table to pull from

    Returns:
        list: A list of dictionaries containing the updated data.
    """
    select_clause = source_query

    filter_clause = ""
    limit_clause = ""
    if last_pull_timestamp:
        filter_clause = f"WHERE updated > '{last_pull_timestamp.strftime('%Y-%m-%d %H:%M:%S')}'"
        limit_clause = "LIMIT 10"

    query = f"{select_clause} {filter_clause} {limit_clause};"
    data = maria_db.execute_query(query)

    return data


def create_text_and_summary(data):
    """
    Create the 'text' and 'summary_text' values based on the data.

    Args:
        data (list): A list of dictionaries containing the data.

    Returns:
        list: A list of dictionaries with 'text' and 'summary_text' values added.
    """
    number_of_rows = len(data)
    for index, row in enumerate(data):
        # Extract text from the PDF file using the provided URL
        url = row["state_url"]
        response = requests.get(url)
        on_fly_mem_obj = io.BytesIO(response.content)
        pdf_file = PdfReader(on_fly_mem_obj)

        # Extract text from each page and concatenate into a single string
        text = ""
        for page in pdf_file.pages:
            text += page.extract_text()

        row["text"] = text
        row["summary_text"] = ""  # Add your logic to create the 'summary_text' value

        if (index + 1) % 10 == 0:
            logger.info(f"Completed text extraction for {index + 1} of {number_of_rows}")
    return data


def save_data_to_rds(model: orm.Base, field_mapping: dict, data: list[dict]):
    """
    Save the data to the RDS table.

    Args:
        model (orm.Base): ORM to describe table to save to
        field_mapping (dict): maps source table columns to target orm
        data (list): A list of dictionaries containing the data to be saved.
    """
    if data:
        rds_db.connect()

        try:
            orm_keys = set(inspect(model).columns.keys()) - {"updated_at"}
            for input_row in data:
                filtered_row = {
                    field_mapping[k]: v
                    for k, v in input_row.items()
                    if field_mapping.get(k) in orm_keys
                }
                rds_row = model(
                    **filtered_row,
                    updated_at=datetime.now()
                )
                rds_db.session.merge(rds_row)
            rds_db.session.commit()

        except IntegrityError as e:
            rds_db.session.rollback()
            raise e
        finally:
            rds_db.close_connection()


def run_data_pipeline():
    PipelineConfig = namedtuple("PipelineConfig", "source_query target_orm field_mapping incremental_load")
    table_mappings = [
        PipelineConfig(
            "SELECT body_id, state_id, role_id, body_name, body_short FROM ls_body",
            orm.LegislativeBody,
            {
                "body_id": "body_id",
                "state_id": "state_id",
                "role_id": "role_id",
                "body_name": "full_name",
                "body_short": "abbr_name"
            },
            False
        ),
        PipelineConfig(
            """SELECT b.bill_id, b.state_abbr, b.session_id, b.body_id, b.status_id, b.state_url, s.progress_desc as status
               FROM lsv_bill_text b LEFT JOIN ls_progress s 
               ON b.status_id = s.progress_event_id
            """,
            orm.Bill,
            {
                "bill_id": "bill_id",
                "state_abbr": "state_code",
                "session_id": "session_id",
                "body_id": "body_id",
                "status_id": "status_id",
                "state_url": "pdf_link"
            },
            True
        )
    ]
    try:
        for pipeline_config in table_mappings:
            logger.info(f"Pulling data for {pipeline_config.target_orm.__tablename__}")

            last_pull_timestamp = None
            if pipeline_config.incremental_load:
                # Get the timestamp of the last pull
                last_pull_timestamp = get_last_pull_timestamp(pipeline_config.target_orm)
                logger.info(f"Target table last updated at {last_pull_timestamp}")

            # Get the updated data from MariaDB
            logger.info(f"Pulling data")
            legiscan_data = get_updated_data(
                source_query=pipeline_config.source_query,
                last_pull_timestamp=last_pull_timestamp
            )
            logger.info(f"Got {len(legiscan_data)} records")

            if pipeline_config.target_orm == orm.Bill:
                # Create 'text' and 'summary_text' values
                logger.info(f"Parsing PDF Data")
                legiscan_data = create_text_and_summary(legiscan_data)

            # Save the data to Postgres RDS
            logger.info(f"Saving Data to RDS")
            save_data_to_rds(
                model=pipeline_config.target_orm,
                field_mapping=pipeline_config.field_mapping,
                data=legiscan_data
            )

    except Exception as e:
        # Get the traceback information
        tb_info = traceback.format_exc()
        # Log the error along with traceback and line information
        logger.error(f"An error occurred: {str(e)}\n{tb_info}")
        # Raise the exception again to halt further execution if desired
        raise


if __name__ == "__main__":
    run_data_pipeline()
