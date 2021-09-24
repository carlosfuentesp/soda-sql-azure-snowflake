import logging
import os

import azure.functions as func
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sodasql.scan.scan_builder import ScanBuilder

ACCOUNT = "<snowflake account>"
DATABASE = "<database>"
WAREHOUSE = "<warehouse>"
SCHEMA = "<schema>"
ROLE = "<role>"

WAREHOUSE_CONFIG = {
        "name" : "snowflake",
        "connection" : 
            {
                "type": "snowflake",
                "username": "env_var(SNOWFLAKE_USERNAME)",
                "password": "env_var(SNOWFLAKE_PASSWORD)",
                "account": ACCOUNT,
                "database": DATABASE,
                "warehouse": WAREHOUSE,
                "schema": SCHEMA
            }
    }


def get_scan_duplicate_config(table):
    return {
        "table_name": table,
        "metrics": ["row_count"],
        "columns": {"PRODUCT_ID": {"metrics": ["duplicate_count"], "tests": ["duplicate_count == 0"]}}
    }


def get_connection():
    logging.info("Connecting to Snowflake...")
    return create_engine(URL(
        user=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    ))


def get_tables(conn):
    logging.info(f"Getting tables from {SCHEMA} schema...")
    query = f"show tables in {DATABASE}.{SCHEMA}"
    df = pd.read_sql_query(query, con=conn.connect())
    return df['name'].tolist()


def run_soda_scan(table, scan_builder):
    scan_builder.scan_yml_dict = get_scan_duplicate_config(table)
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_test_failures():
        failures = scan_result.get_test_failures_count()
        logging.error(f"Soda Scan found {failures} errors in your table {table}!")
    else:
        logging.info(f"Everything looks good for table: {table}")
    logging.info(scan_result.to_json())


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_connection()
        tables = get_tables(conn)
        scan_builder = ScanBuilder()
        scan_builder.warehouse_yml_dict = WAREHOUSE_CONFIG
        for table in tables:
            run_soda_scan(table.lower(), scan_builder)
        return func.HttpResponse("This HTTP triggered function executed successfully.")
    except Exception as e:
        logging.error(f"Error :{e}")
        return 0
        