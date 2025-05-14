from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, date_trunc, flatten
from .config import SNOWFLAKE_CONFIG

def get_session():
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()


def load_table(session, table_name):
    t = session.table(table_name)
    stats = session.sql(f"SELECT MIN(INSERTED_AT) MN, MAX(INSERTED_AT) MX FROM {table_name}")\
                   .collect()[0].as_dict()
    return t, stats
