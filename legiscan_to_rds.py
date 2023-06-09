import logging
import traceback

from datetime import datetime
from configparser import ConfigParser
from sqlalchemy import text

from database_connection import MariaDBLocal, PostgresRDS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Read configuration file
config = ConfigParser()
config.read("config.ini")

# Read database credentials
maria_host = config.get("database", "maria_host")
maria_user = config.get("database", "maria_user")
maria_password = config.get("database", "maria_password")
maria_database = config.get("database", "maria_database")

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
    "bill_id", "state", "session_id", "body_id", "status_id",
    "pdf_link", "text", "summary_text", "updated_at"
]


def parse_data():
    try:
        # Read the timestamp of the last pull
        query = f"SELECT MAX(updated_at) FROM {rds_table}"
        result = rds_db.execute_query(query)
        last_pull_timestamp = result[0]["max"] or datetime.min

        # Read data from MariaDB that has been updated since the last pull
        maria_query = f"""
          SELECT {', '.join(maria_columns)}  FROM {maria_table}
          WHERE updated > '{last_pull_timestamp.strftime('%Y-%m-%d %H:%M:%S')}'"""
        maria_data = maria_db.execute_query(maria_query)

        # Save data to Postgres RDS
        queries = []
        query_template = text(f"""
            INSERT INTO {rds_table} ({', '.join(rds_columns)})
            VALUES (
                :bill_id, :state_code, :session_id, :body_id, :status_id, :pdf_link, 
                :text, :summary_text, :updated_at
            )
            ON CONFLICT (bill_id) DO UPDATE
            SET (
                state_abbr, session_id, body_id,
                status_id, state_url, updated_at
            ) = (
                :state_code, :session_id, :body_id,
                :status_id, :pdf_link, NOW()
            )
        """)

        for row in maria_data:
            parsed_data = {
                'bill_id': row["bill_id"],
                'state_code': row["state_abbr"],
                'session_id': row["session_id"],
                'body_id': row["body_id"],
                'status_id': row["status_id"],
                'pdf_link': row["state_url"],
                'text': "",
                'summary_text': "",
                'updated_at': datetime.now()
            }

            queries.append((query_template, parsed_data))

        # Execute the transaction
        rds_db.execute_transaction(queries)

    except Exception as e:
        # Get the traceback information
        tb_info = traceback.format_exc()
        # Log the error along with traceback and line information
        logger.error(f"An error occurred: {str(e)}\n{tb_info}")
        # Raise the exception again to halt further execution if desired
        raise


if __name__ == "__main__":
    parse_data()
