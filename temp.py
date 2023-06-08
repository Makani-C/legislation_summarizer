import psycopg2

# RDS database connection details
host = "legislation-database.cz3q8p7r1eqy.us-west-2.rds.amazonaws.com"
port = 5432
database = "legislation-database"
user = "postgres"
password = "1VyeFXsR82sAPuj1"

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
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cursor = connection.cursor()

    # Execute the create table query
    cursor.execute(create_table_query)
    connection.commit()

    # Close the database connection
    cursor.close()
    connection.close()

    print("The 'bills' table was created successfully!")

except (psycopg2.Error) as e:
    print(f"An error occurred while creating the 'bills' table: {e}")
