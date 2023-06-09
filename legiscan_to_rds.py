import logging

from datetime import datetime
from configparser import ConfigParser
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
    "bill_id", "state_abbr", "session_id", "body_id", "status_id",
    "state_url", "text", "summary_text"
]


def parse_data():
    try:
        # Read the timestamp of the last pull
        query = f"SELECT MAX(updated_at) FROM {rds_table}"
        result = rds_db.execute_query(query)
        last_pull_timestamp = result[0][0] if result[0][0] is not None else datetime.min
        print(last_pull_timestamp)

        # Read data from MariaDB that has been updated since the last pull
        maria_query = f"SELECT {', '.join(maria_columns)} FROM {maria_table} WHERE updated > %s LIMIT 10"
        maria_data = maria_db.execute_query(maria_query, (last_pull_timestamp,))

        # Save data to Postgres RDS
        for row in maria_data:
            # Perform data parsing based on mapping
            parsed_data = [
                row["bill_id"],
                row["state_abbr"],
                row["session_id"],
                row["body_id"],
                row["status_id"],
                row["state_url"],
                "",
                ""
            ]

            # Insert or update data into the bills table in Postgres RDS
            rds_query = f"""
                INSERT INTO {rds_table} ({', '.join(rds_columns)})
                VALUES {tuple(parsed_data)}
                ON CONFLICT (bill_id) DO UPDATE
                SET (state_abbr, session_id, body_id, status_id, state_url, updated_at) =
                    (EXCLUDED.state_abbr, EXCLUDED.session_id, EXCLUDED.body_id, EXCLUDED.status_id, EXCLUDED.state_url, NOW())
            """
            rds_db.execute_query(rds_query)

    except Exception as e:
        # Handle errors appropriately
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    parse_data()
