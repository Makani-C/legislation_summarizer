from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError


def in_session(func):
    """Decorator that ensures a database connection is established before executing a method."""
    def wrapper(self, *args, **kwargs):
        if self.session is None or not self.session.is_active:
            self.connect()
        try:
            return func(self, *args, **kwargs)
        except IntegrityError as e:
            self.session.rollback()
            raise e
        finally:
            self.close_connection()
    return wrapper


class DatabaseConnector:
    """Base class for connecting to a database."""
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = None
        self.session = None

    def sessionmaker(self):
        connection_string = self.get_connection_string()
        self.engine = create_engine(connection_string)
        return sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )

    def connect(self):
        connection_string = self.get_connection_string()
        self.engine = create_engine(connection_string)
        session = sessionmaker(bind=self.engine)
        self.session = session()
        print(f"Connected to {self}")

    def close_connection(self):
        self.session.close()
        print(f"Closed connection to {self}")

    def get_connection_string(self):
        raise NotImplementedError("Subclasses must implement get_connection_string()")

    @in_session
    def execute_query(self, query: str, params=None):
        """ Execute a query and return the result as a list of dictionaries.

        Args:
            query (str): The SQL query to execute.
            params (dict): Optional parameters to be passed to the query.

        Returns:
            list: A list of dictionaries representing the query result.
        """
        result = self.session.execute(text(query), params)

        column_names = result.keys()
        data = [dict(zip(column_names, row)) for row in result]

        return data

    @in_session
    def execute_orm_query(self, query):
        """ Execute an ORM query and return the result.

        Args:
            query: The SQLAlchemy query to execute.

        Returns:
            list: A list of ORM query results.
        """
        return self.session.execute(query).scalars().all()


class MariaDB(DatabaseConnector):

    def __repr__(self):
        return f"MariaDB at {self.host}/{self.database}"

    def get_connection_string(self):
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"


class PostgresDB(DatabaseConnector):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, user, password, database)
        self.port = port

    def __repr__(self):
        return f"RDS PostgresDB at {self.host}/{self.database}"

    def get_connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @in_session
    def execute_transaction(self, queries: list):
        """ Execute a transaction with a list of queries.

        Args:
            queries (list): A list of tuples containing (query, params) pairs.

        Raises:
            Exception: If an error occurs during the transaction.
        """
        if self.session._transaction is None or not self.session._transaction.is_active:
            self.session.begin()
        try:
            for query, params in queries:
                self.session.execute(query, params)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
