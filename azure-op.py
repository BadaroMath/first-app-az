import pyodbc
from msrestazure.azure_active_directory import MSIAuthentication

class AzureSQLPartitionedTableSender:
    def __init__(self, server_name, database_name, table_name):
        self.server_name = server_name
        self.database_name = database_name
        self.table_name = table_name
        self.auth = MSIAuthentication()

    def delete_table(self, table_name):
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server_name};Database={self.database_name};Authentication=ActiveDirectoryMsi"
        conn = pyodbc.connect(conn_str, auth=self.auth)
        cursor = conn.cursor()

        delete_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(delete_query)

        conn.commit()
        conn.close()

    def create_table(self, table_name, columns):
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server_name};Database={self.database_name};Authentication=ActiveDirectoryMsi"
        conn = pyodbc.connect(conn_str, auth=self.auth)
        cursor = conn.cursor()

        columns_str = ", ".join(columns)
        create_query = f"CREATE TABLE {table_name} ({columns_str})"
        cursor.execute(create_query)

        conn.commit()
        conn.close()

    def append_data(self, table_name, data):
            conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server_name};Database={self.database_name};Authentication=ActiveDirectoryMsi"
            conn = pyodbc.connect(conn_str, auth=self.auth)
            cursor = conn.cursor()

            columns = data[0].keys()
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['?' for _ in columns])

            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            cursor.executemany(insert_query, [tuple(item[column] for column in columns) for item in data])

            conn.commit()
            conn.close()

    def read_table(self, table_name):
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server_name};Database={self.database_name};Authentication=ActiveDirectoryMsi"
        conn = pyodbc.connect(conn_str, auth=self.auth)

        select_query = f"SELECT * FROM {table_name}"
        cursor = conn.cursor()
        cursor.execute(select_query)

        rows = cursor.fetchall()

        conn.close()

        return rows

    def send_data(self, grouped_data):
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server_name};Database={self.database_name};Authentication=ActiveDirectoryMsi"
        conn = pyodbc.connect(conn_str, auth=self.auth)
        cursor = conn.cursor()

        for date, items in grouped_data.items():
            partition_create_query = f"ALTER TABLE {self.table_name} SWITCH PARTITION {date} TO {self.table_name}_Staging"
            cursor.execute(partition_create_query)

            truncate_query = f"TRUNCATE TABLE {self.table_name}_Staging"
            cursor.execute(truncate_query)

            insert_query = f"INSERT INTO {self.table_name}_Staging (date, ...) VALUES (?, ...)"
            cursor.executemany(insert_query, [(item['date'], ...) for item in items])

            partition_switch_query = f"ALTER TABLE {self.table_name}_Staging SWITCH PARTITION {date} TO {self.table_name}"
            cursor.execute(partition_switch_query)

        conn.commit()
        conn.close()
