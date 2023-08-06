import io
import sys
import os
import logging
import requests
import traceback

from datetime import datetime
from PyPDF2 import PdfReader
from sqlalchemy import func, inspect
from sqlalchemy.exc import IntegrityError

filepath = os.path.realpath(__file__)
root_dir = os.path.dirname(os.path.dirname(filepath))
sys.path.append(root_dir)

from database import connectors, orm
from summarizer import summarize_text

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize MariaDB and RDS instances
maria_db = connectors.LegiscanDBConnection()
rds_db = connectors.RDSConnection()


def get_last_pull_timestamp(model: orm.Base):
    """Get the timestamp of the last pull from the RDS table.

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


def get_updated_data(table_config: dict, last_pull_timestamp: datetime = None):
    """
    Get the updated data from the MariaDB table.

    Args:
        table_config (dict): Configuration for pulling legiscan data
        last_pull_timestamp (datetime.datetime): The timestamp of the last pull.

    Returns:
        list: A list of dictionaries containing the updated data.
    """
    select_clause = table_config["source_query"]

    filter_clause = table_config.get("filter_clause", "")
    if last_pull_timestamp:
        if not filter_clause:
            filter_clause = "WHERE "
        else:
            filter_clause = f"{filter_clause} AND "
        last_pull_string = last_pull_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        filter_clause = f"{filter_clause} updated > '{last_pull_string}'"

    limit_clause = table_config.get("limit_clause", "")

    query = f"{select_clause} {filter_clause} {limit_clause};"
    data = maria_db.execute_query(query)

    return data


def create_text_and_summary(data: list) -> list:
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
        bill_text = ""
        for page in pdf_file.pages:
            bill_text += page.extract_text()

        row["text"] = bill_text
        row["summary_text"] = summarize_text(bill_text)

        if (index + 1) % 10 == 0:
            logger.info(
                f"Completed text extraction for {index + 1} of {number_of_rows}"
            )

    return data


def save_data_to_rds(model: orm.Base, field_mapping: dict, data: list) -> None:
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
                rds_row = model(**filtered_row, updated_at=datetime.now())
                rds_db.session.merge(rds_row)
            rds_db.session.commit()

        except IntegrityError as e:
            rds_db.session.rollback()
            raise e
        finally:
            rds_db.close_connection()


def run_data_pipeline(limit: int = None, state_list: list = None) -> None:
    """
    Run the data pipeline to update target tables with data from the source.

    This function performs the ETL (Extract, Transform, Load) process to update target tables
    using data fetched from a source query. It fetches data from MariaDB, processes it (if needed),
    and saves it to a target table in Postgres RDS.

    The ETL process can include incremental loads and data parsing for specific target tables.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the ETL process.
    """
    table_mappings = {
        # Configuration for the 'LegislativeBody' table
        "body": {
            "source_query": "SELECT body_id, state_id, role_id, body_name, body_short FROM ls_body",
            "target_orm": orm.LegislativeBody,
            "field_mapping": {
                "body_id": "body_id",
                "state_id": "state_id",
                "role_id": "role_id",
                "body_name": "full_name",
                "body_short": "abbr_name",
            },
            "incremental_load": False,
        },
        # Configuration for the 'Bill' table
        "bill": {
            "source_query": """SELECT b.bill_id, b.state_abbr, b.session_id, b.body_id, b.status_id, b.state_url, s.progress_desc as status
                               FROM lsv_bill_text b LEFT JOIN ls_progress s 
                               ON b.status_id = s.progress_event_id
                            """,
            "target_orm": orm.Bill,
            "field_mapping": {
                "bill_id": "bill_id",
                "state_abbr": "state_code",
                "session_id": "session_id",
                "body_id": "body_id",
                "status_id": "status_id",
                "state_url": "pdf_link",
                "text": "text",
                "summary_text": "summary_text"
            },
            "incremental_load": False, # TODO - implement consistent incremental load pipeline
        },
    }
    if state_list:
        state_list_string = "', '".join(state_list)
        table_mappings["bill"]["filter_clause"] = f"WHERE state_abbr IN ('{state_list_string}')"
    if limit:
        table_mappings["bill"]["limit_clause"] = f"LIMIT {limit}"

    try:
        for table_id, pipeline_config in table_mappings.items():
            logger.info(f"Updating {pipeline_config['target_orm'].__tablename__}")

            # Check if incremental load is enabled and get the timestamp of the last pull
            last_pull_timestamp = None
            if pipeline_config["incremental_load"]:
                last_pull_timestamp = get_last_pull_timestamp(pipeline_config["target_orm"])
                logger.info(f"Target table last updated at {last_pull_timestamp}")

            # Fetch updated data from MariaDB
            logger.info(f"Pulling data from legiscan_api")
            legiscan_data = get_updated_data(
                table_config=pipeline_config,
                last_pull_timestamp=last_pull_timestamp,
            )
            logger.info(f"Got {len(legiscan_data)} records")

            # Perform additional processing for the Bill text
            if pipeline_config["target_orm"] == orm.Bill:
                logger.info(f"Parsing PDF Data")
                legiscan_data = create_text_and_summary(legiscan_data)

            # Save the data to Postgres RDS
            logger.info(f"Saving Data to RDS")
            save_data_to_rds(
                model=pipeline_config["target_orm"],
                field_mapping=pipeline_config["field_mapping"],
                data=legiscan_data,
            )

    except Exception as e:
        # Get the traceback information
        tb_info = traceback.format_exc()
        # Log the error along with traceback and line information
        logger.error(f"An error occurred: {str(e)}\n{tb_info}")
        # Raise the exception again to halt further execution if desired
        raise


if __name__ == "__main__":
    run_data_pipeline(state_list=["US"])
