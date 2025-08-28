# src/helpers/postgresql_hook.py

import psycopg2
from psycopg2 import pool
import logging
import configparser
import os

class PostgreSQLHook:
    def __init__(self, config_file='config.ini', section='DATABASE_PSQL', dbname='staging_db'):
        # Get the path to the config file
        self.config_file = self._get_config_path(config_file)
        self.section = section
        self.dbname = dbname
        self.conn_pool = self._create_connection_pool()


    def _get_config_path(self, config_file):
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the full path to the config file
        return os.path.join(current_dir, '..', '..', 'config', config_file)


    def _create_connection_pool(self):
        config = configparser.ConfigParser()
        config.read(self.config_file)
        db_config = config[self.section]

        return psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,  # Adjust the maxconn according to your needs
            dbname=self.dbname,
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
        )

    def _get_connection(self):
        return self.conn_pool.getconn()


    def _release_connection(self, conn):
        self.conn_pool.putconn(conn)

    def  test_connection(self):
        conn = self._get_connection()
        self._release_connection(conn)
        if conn:
            return True
        else:
            return False
        

    def execute_query(self, query, params=None, batch_size=100):
        """Execute a query. Can be used for single or multiple executions (batch).
        
        Args:
            query (str): The SQL query to execute.
            params (tuple or list): 
                - A tuple for a single set of parameters.
                - A list of tuples for batch execution (multiple sets of parameters).
        """
        conn = self._get_connection()
        try:
            with conn:
                with conn.cursor() as cursor:
                    if isinstance(params, list):
                        for i in range(0, len(params), batch_size):
                            cursor.executemany(query, params[i:i + batch_size])
                    else:
                        cursor.execute(query, params)
        except psycopg2.Error as e:
            logging.error(f"Error executing query: {e}")
            raise
        finally:
            self._release_connection(conn)


    def fetch_data_in_batches(self, query, params=None, batch_size=100):
        """Fetch data from the database in batches."""
        conn = self._get_connection()
        try:
            with conn:
                with conn.cursor() as cursor:
                    # logging.info(f"Executing fetch query '{query}' with params {params} in batches of {batch_size}.")
                    
                    cursor.execute(query, params)
                    
                    total_rows = cursor.rowcount  # Get total number of rows
                    total_batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)  # Calculate total batches
                    
                    batch_number = 1  # Initialize batch number
                    
                    while True:
                        data = cursor.fetchmany(batch_size)
                        if not data:
                            break
                        
                        # logging.info(f"Fetched batch {batch_number}/{total_batches} of {len(data)} rows for query '{query}' and params {params}.")
                        yield data  # Yield the batch of data
                        
                        batch_number += 1  # Increment batch number
        except psycopg2.Error as e:
            logging.error(f"Error fetching data with query '{query}': {e}")
            raise
        finally:
            self._release_connection(conn)


    def fetch_all_data(self, query, params=None):
        """Fetch all data from the database at once."""
        conn = self._get_connection()
        try:
            with conn:
                with conn.cursor() as cursor:
                    # logging.info(f"Executing fetch query '{query}' with params {params}.")
                    cursor.execute(query, params)
                    data = cursor.fetchall()  # Fetch all data at once
                    logging.info(f"Fetched {len(data)} rows for params {params}.")
                    return data  # Return the fetched data as a list
        except psycopg2.Error as e:
            logging.error(f"Error fetching data with query '{query}': {e}")
            # raise
            return None
        finally:
            self._release_connection(conn)


    def insert_data(self, table_name, columns, primary_key_columns, data, batch_size=100):
        """Insert data into a table.
        
        Args:
            table_name (str): The table name.
            columns (list): A list of column names.
            primary_key_columns (list): A list of primary key column names to check for conflicts.
            data (list): A list of tuples where each tuple contains the values for each column.
            batch_size (int): The number of rows to insert at once.
        """
        # print how many rows are being inserted in which table
        # print(f"Inserting {len(data)} rows into table '{table_name}'...")
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s' for _ in columns])

        # Construct the update clause, ensuring the primary key is excluded
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in primary_key_columns])

        if primary_key_columns and update_clause:
            # Construct the query to insert data with conflict handling
            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders}) 
                ON CONFLICT ({', '.join(primary_key_columns)}) 
                DO UPDATE SET {update_clause};
            """
        elif primary_key_columns:
            # If there are primary key columns but no update clause, do nothing on conflict
            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders}) 
                ON CONFLICT ({', '.join(primary_key_columns)}) 
                DO NOTHING;
            """
        else:
            # If no primary key columns, just insert the data
            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders});
            """

        # Establish connection to the database
        conn = self._get_connection()
        try:
            with conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(data), batch_size):
                        cursor.executemany(query, data[i:i + batch_size])
                        # print(f"Inserted batch {i // batch_size + 1}/{len(data) // batch_size + 1} with {cursor.rowcount} rows into table '{table_name}'.")
        except psycopg2.Error as e:
            logging.error(f"Error inserting data into table '{table_name}': {e}")
            print(e.pgcode)
            # Provide more details of the error:
            print(e.pgcode)
            print(e.pgerror)
        finally:
            # Release the connection after the operation
            self._release_connection(conn)



    def create_schema(self, schema_name):
        """Create a new schema.
        
        Args:
            schema_name (str): The schema name.
        """
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        self.execute_query(query)
        

    def create_table(self, schema, table_name, columns, primary_key=None):
        """Create a new table in a specified schema.
        
        Args:
            schema (str): The schema name.
            table_name (str): The table name.
            columns (dict): A dictionary where the keys are column names and the values are column types.
            primary_key (list, optional): List of column names that form the primary key.
        """
        # Construct the column definitions
        columns_with_types = ', '.join([f"{col} {col_type}" for col, col_type in columns.items()])

        # Add PRIMARY KEY if specified
        if primary_key:
            primary_key_clause = f", PRIMARY KEY ({', '.join(primary_key)})"
        else:
            primary_key_clause = ""

        # Build the final SQL query
        query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({columns_with_types}{primary_key_clause});"

        # Execute the query
        self.execute_query(query)


    def add_column(self, schema, table_name, column_name, column_type):
        """Add a new column to an existing table.
        
        Args:
            schema (str): The schema name.
            table_name (str): The table name.
            column_name (str): The column name.
            column_type (str): The column type.
        """
        query = f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
        self.execute_query(query)

    def schema_exists(self, schema_name):
        """Check if a schema exists.
        
        Args:
            schema_name (str): The schema name.
        
        Returns:
            bool: True if the schema exists, False otherwise.
        """
        query = (
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.schemata "
            "WHERE schema_name = %s"
            ")"
        )
        # Fetch the result using parameters to avoid SQL injection
        result = self.fetch_all_data(query, params=(schema_name,))
        # Return the first element of the first row
        return result[0][0]
    

    def table_exists(self, schema, table_name):
        """Check if a table exists in a schema.
        
        Args:
            schema (str): The schema name.
            table_name (str): The table name.
        
        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = (
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s"
            ")"
        )
        # Fetch the result using parameters to avoid SQL injection
        result = self.fetch_all_data(query, params=(schema, table_name))
        # Return the first element of the first row
        return result[0][0]  # Assuming result is a list of one tuple

    def column_exists(self, schema, column_name, table_name):
        """Check if a column exists in a table.
        
        Args:
            schema (str): The schema name.
            column_name (str): The column name.
            table_name (str): The table name.
        
        Returns:
            bool: True if the column exists, False otherwise.
        """
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s);"
        result = self.fetch_all_data(query, params=(schema, table_name, column_name))
        return result[0][0]  # Assuming result is a list of one tuple
