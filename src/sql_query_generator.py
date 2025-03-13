
import snowflake.connector
import os

# Establish Snowflake connection
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn

# Function to generate SQL query using Snowflake Cortex
def generate_sql_query(prompt):
    conn = get_snowflake_connection()
    try:
        query = f"""
        SELECT LLM_COMPLETE('Generate an optimized SQL query for: {prompt}') AS sql_query;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]
    finally:
        conn.close()
