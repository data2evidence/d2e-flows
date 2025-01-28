import os

def raw_sql_files(conn, dir_root, sql_files):
    for sql_file in sql_files:
        with open(os.path.join(dir_root, sql_file), 'r') as file:
            query = file.read()
            conn.execute(query)

def create_schema(conn, schema_name:str):
    conn.execute(f"""
    DROP SCHEMA IF EXISTS {schema_name} CASCADE ;
    CREATE SCHEMA {schema_name} ;
    """)