from flask import Flask, request, jsonify
import psycopg2
import requests

app = Flask(__name__)

# Database connection configuration
db_config = {
    "database": "your_database_name",
    "user": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port"
}

# API endpoint for refreshing the table
@app.route('/refresh_table', methods=['POST'])
def refresh_table():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Drop the existing table
        drop_table_query = "DROP TABLE IF EXISTS your_table_name"
        cursor.execute(drop_table_query)
        conn.commit()

        # Recreate the table
        create_table_query = """
            CREATE TABLE your_table_name (
                column1 data_type1,
                column2 data_type2,
                ...
            )
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Fetch data from the source and insert into the table
        data = fetch_data_from_source()  # Implement your own data fetching logic
        insert_query = "INSERT INTO your_table_name (column1, column2, ...) VALUES (%s, %s, ...)"
        cursor.executemany(insert_query, data)
        conn.commit()

        # Close the database connection
        cursor.close()
        conn.close()

        return jsonify({"message": "Table refreshed successfully"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Function to fetch data from the source (Example implementation)
def fetch_data_from_source():
    # Implement your logic to fetch data from the source
    # and return it as a list of tuples or a list of dictionaries.
    # Example:
    data = [
        (value1, value2, ...),
        (value1, value2, ...),
        ...
    ]
    return data

# Start the Flask server
if __name__ == '__main__':
    app.run()   