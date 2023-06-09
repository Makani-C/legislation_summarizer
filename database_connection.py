from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class DatabaseConnector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = None
        self.session = None

    def connect(self):
        connection_string = self.get_connection_string()
        self.engine = create_engine(connection_string)
        session = sessionmaker(bind=self.engine)
        self.session = session()
        print(f"Connected to {self.get_database_type()} database.")

    def close_connection(self):
        self.session.close()
        self.engine.dispose()
        print(f"Connection to {self.get_database_type()} database closed.")

    def get_connection_string(self):
        raise NotImplementedError("Subclasses must implement get_connection_string()")

    def get_database_type(self):
        raise NotImplementedError("Subclasses must implement get_database_type()")

    @staticmethod
    def connection_required(func):
        def wrapper(self, *args, **kwargs):
            if self.session is None or not self.session.is_active:
                self.connect()
            try:
                return func(self, *args, **kwargs)
            finally:
                self.close_connection()
        return wrapper

    @connection_required
    def execute_query(self, query, params=None):
        result = self.session.execute(text(query), params)

        column_names = result.keys()
        data = [dict(zip(column_names, row)) for row in result]

        return data

    @connection_required
    def execute_transaction(self, queries):
        self.session.begin()
        try:
            for query in queries:
                self.session.execute(text(query))
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e


class MariaDBLocal(DatabaseConnector):
    def get_connection_string(self):
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"

    def get_database_type(self):
        return "local MariaDB"


class PostgresRDS(DatabaseConnector):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, user, password, database)
        self.port = port

    def get_connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_database_type(self):
        return "Amazon RDS"

    def escape_string(self, value):
        return self.session.query(text("(:value)::text").params(value=value)).scalar()
