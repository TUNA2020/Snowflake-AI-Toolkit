import pandas as pd
from src.sql_query_generator import get_snowflake_connection

# Execute AI-generated SQL query
def execute_sql_query(sql_query):
    conn = get_snowflake_connection()
    try:
        df = pd.read_sql(sql_query, conn)
        return df
    except Exception as e:
        return f"Error executing query: {e}"
    finally:
        conn.close()
