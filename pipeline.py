import io
import logging
import requests
import traceback

from datetime import datetime
from configparser import ConfigParser
from PyPDF2 import PdfReader
from sqlalchemy import text

from database_connection import MariaDBLocal, PostgresRDS

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Read configuration file
config = ConfigParser()
config.read("config.ini")

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
maria_db = MariaDBLocal(
    host=maria_host,
    user=maria_user,
    password=maria_password,
    database=maria_database
)
rds_db = PostgresRDS(
    host=rds_host,
    port=rds_port,
    user=rds_user,
    password=rds_password,
    database=rds_database
)


# Table and Column Names
maria_table = "lsv_bill_text"
maria_columns = [
    "bill_id", "state_abbr", "bill_number", "text_id", "bill_text_size",
    "bill_text_date", "bill_text_type_id", "bill_text_name", "bill_text_sort",
    "bill_text_mime_id", "mime_type", "mime_ext", "bill_text_hash", "legiscan_url",
    "state_url", "local_copy", "local_fragment", "state_id", "state_name",
    "session_id", "body_id", "current_body_id", "bill_type_id", "status_id",
    "pending_committee_id", "created", "updated"
]

rds_table = "bills"
rds_columns = [
    "bill_id", "state_code", "session_id", "body_id", "status_id",
    "pdf_link", "text", "summary_text", "updated_at"
]


def get_last_pull_timestamp():
    """ Get the timestamp of the last pull from the RDS table.

    Returns:
        datetime.datetime: The last pull timestamp.
    """
    query = f"SELECT MAX(updated_at) FROM {rds_table}"
    result = rds_db.execute_query(query)
    last_pull_timestamp = result[0]["max"] or datetime.min

    return last_pull_timestamp


def get_updated_data(last_pull_timestamp):
    """
    Get the updated data from the MariaDB table.

    Args:
        last_pull_timestamp (datetime.datetime): The timestamp of the last pull.

    Returns:
        list: A list of dictionaries containing the updated data.
    """
    query = f"""
        SELECT {', '.join(maria_columns)}  FROM {maria_table}
        WHERE updated > '{last_pull_timestamp.strftime('%Y-%m-%d %H:%M:%S')}' 
        """
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


def save_data_to_rds(data):
    """
    Save the data to the RDS table.

    Args:
        data (list): A list of dictionaries containing the data to be saved.
    """
    queries = []
    query_template = text(f"""
        INSERT INTO {rds_table} ({', '.join(rds_columns)})
        VALUES (
            :bill_id, :state_code, :session_id, :body_id, :status_id, 
            :pdf_link, :text, :summary_text, :updated_at
        )
        ON CONFLICT (bill_id) DO UPDATE
        SET (
            state_code, session_id, body_id, status_id, 
            pdf_link, text, summary_text, updated_at
        ) = (
            :state_code, :session_id, :body_id, :status_id,
            :pdf_link, :text, :summary_text, :updated_at
        )
    """)

    for row in data:
        parsed_data = {
            "bill_id": row["bill_id"],
            "state_code": row["state_abbr"],
            "session_id": row["session_id"],
            "body_id": row["body_id"],
            "status_id": row["status_id"],
            "pdf_link": row["state_url"],
            "text": row["text"],
            "summary_text": row["summary_text"],
            "updated_at": datetime.now()
        }

        queries.append((query_template, parsed_data))

    # Execute the transaction
    rds_db.execute_transaction(queries)


def run_data_pipeline():
    try:
        # Get the timestamp of the last pull
        last_pull_timestamp = get_last_pull_timestamp()

        # Get the updated data from MariaDB
        logger.info(f"Pulling legiscan data since {last_pull_timestamp}")
        legiscan_data = get_updated_data(last_pull_timestamp)
        logger.info(f"Got {len(legiscan_data)} records")

        # Create 'text' and 'summary_text' values
        logger.info(f"Parsing PDF Data")
        parsed_data = create_text_and_summary(legiscan_data)

        # Save the data to Postgres RDS
        logger.info(f"Saving Data to RDS")
        save_data_to_rds(parsed_data)

    except Exception as e:
        # Get the traceback information
        tb_info = traceback.format_exc()
        # Log the error along with traceback and line information
        logger.error(f"An error occurred: {str(e)}\n{tb_info}")
        # Raise the exception again to halt further execution if desired
        raise


if __name__ == "__main__":
    run_data_pipeline()