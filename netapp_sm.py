import psycopg2
import requests

# Database connection configuration
db_config = {
    "database": "your_database_name",
    "user": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port"
}

# API endpoint configuration
api_url = "https://your_api_endpoint"

# Connect to the database
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Fetch data from the API endpoint
response = requests.get(api_url)
data = response.json()

# Create or update the table
create_table_query = """
    CREATE TABLE IF NOT EXISTS your_table_name (
        column1 data_type1,
        column2 data_type2,
        ...
    )
"""
cursor.execute(create_table_query)
conn.commit()

# Update the table with the latest API output
for item in data:
    # Extract data from the API response
    column1_value = item["column1"]
    column2_value = item["column2"]
    # ...

    # Insert or update the row in the table
    update_query = """
        INSERT INTO your_table_name (column1, column2, ...)
        VALUES (%s, %s, ...)
        ON CONFLICT (column1) DO UPDATE
        SET column2 = EXCLUDED.column2, ...
    """
    values = (column1_value, column2_value, ...)
    cursor.execute(update_query, values)

conn.commit()

# Close the database connection
cursor.close()
conn.close()
