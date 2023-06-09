import psycopg2

from database_connection import PostgresRDS

# RDS database connection details
host = "legislation-db.cz3q8p7r1eqy.us-west-2.rds.amazonaws.com"
port = 5432
database = "legislation"
user = "postgres"
password = "aaaaaaaa"

# SQL statement to create the bills table
create_table_query = """
    CREATE TABLE IF NOT EXISTS bills (
        bill_id INT PRIMARY KEY,
        state VARCHAR(50),
        session_id INT,
        body_id INT,
        status_id INT,
        pdf_link VARCHAR(255),
        text TEXT,
        summary_text TEXT,
        updated_at TIMESTAMP
    );
"""

try:
    rds_db = PostgresRDS(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    # Execute the create table query
    rds_db.execute_query(create_table_query)

    print("The 'bills' table was created successfully!")

except (psycopg2.Error) as e:
    print(f"An error occurred while creating the 'bills' table: {e}")
