import mysql.connector
import sys
import os
import re
from mysql.connector import Error
import pandas as pd

def connect_to_mysql(host, database, user, password):
    """
    Establish a connection to MySQL database
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        if connection.is_connected():
            db_info = connection.server_info  # Using property instead of deprecated method
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"Connected to database: {record[0]}")
            return connection
        
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_table(connection, table_name, column_definitions):
    """
    Create a table in the database if it doesn't exist
    """
    try:
        cursor = connection.cursor()
        
        # Generate the CREATE TABLE SQL statement
        columns_sql = ", ".join([f"{name} {data_type}" for name, data_type in column_definitions])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
        
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created or already exists")
        return True
        
    except Error as e:
        print(f"Error creating table: {e}")
        return False

def get_column_types_from_csv(csv_file_path):
    """
    Infer column types from CSV first row
    Returns a list of (column_name, mysql_data_type) tuples
    """
    try:
        # Read the entire CSV to better infer data types
        df = pd.read_csv(csv_file_path)
        column_types = []
        
        for column in df.columns:
            # Clean column name (replace problematic characters)
            clean_column = column.lower()
            clean_column = clean_column.replace(" ", "_")
            clean_column = clean_column.replace("(", "_")
            clean_column = clean_column.replace(")", "_")
            clean_column = clean_column.replace("-", "_")
            clean_column = clean_column.replace("%", "pct")
            clean_column = clean_column.replace("$", "usd")
            clean_column = clean_column.replace(".", "_")
            clean_column = clean_column.replace(",", "_")
            clean_column = clean_column.rstrip("_")  # Remove trailing underscores
            
            # Ensure clean_column is a valid MySQL identifier
            if clean_column[0].isdigit():
                clean_column = "col_" + clean_column
            
            # Check data type with more cautious approach
            if pd.api.types.is_numeric_dtype(df[column]):
                # Check if any values are actually floats
                has_floats = False
                has_large_values = False
                
                if not df[column].dropna().empty:
                    # Check if any value is float
                    has_floats = any(isinstance(x, float) and not x.is_integer() for x in df[column].dropna())
                    
                    # Check if any value is large
                    if pd.api.types.is_integer_dtype(df[column]):
                        has_large_values = df[column].max() > 2147483647 or df[column].min() < -2147483648
                
                if has_large_values:
                    column_types.append((clean_column, "BIGINT"))
                elif has_floats or not pd.api.types.is_integer_dtype(df[column]):
                    column_types.append((clean_column, "DOUBLE"))  # Use DOUBLE instead of FLOAT for better precision
                else:
                    column_types.append((clean_column, "INT"))
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                column_types.append((clean_column, "DATETIME"))
            else:
                # For text, calculate max length from all rows
                max_length = df[column].astype(str).str.len().max()
                
                # For very short text, use VARCHAR with exact max length
                if max_length < 50:
                    safe_length = max_length + 10  # Small buffer
                # For medium text use reasonable buffer
                elif max_length < 255:
                    safe_length = min(max_length * 1.5, 255)  # Add 50% buffer up to VARCHAR(255)
                # For larger text, use TEXT type instead of VARCHAR
                else:
                    # Use appropriate text type based on size
                    if max_length <= 65535:
                        column_types.append((clean_column, "TEXT"))
                    elif max_length <= 16777215:
                        column_types.append((clean_column, "MEDIUMTEXT"))
                    else:
                        column_types.append((clean_column, "LONGTEXT"))
                    continue  # Skip the VARCHAR logic below
                    
                column_types.append((clean_column, f"VARCHAR({safe_length})"))
                
        return column_types, df.columns.tolist()
    
    except Exception as e:
        print(f"Error inferring column types: {e}")
        return None

def insert_csv_data(connection, csv_file_path, table_name):
    """
    Read data from CSV and insert into MySQL table
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        # Clean column names the same way as in get_column_types_from_csv
        cleaned_columns = []
        for col in df.columns:
            clean_col = col.lower()
            clean_col = clean_col.replace(" ", "_")
            clean_col = clean_col.replace("(", "_")
            clean_col = clean_col.replace(")", "_")
            clean_col = clean_col.replace("-", "_")
            clean_col = clean_col.replace("%", "pct")
            clean_col = clean_col.replace("$", "usd")
            clean_col = clean_col.replace(".", "_")
            clean_col = clean_col.replace(",", "_")
            clean_col = clean_col.rstrip("_")  # Remove trailing underscores
            
            # Ensure clean_column is a valid MySQL identifier
            if clean_col[0].isdigit():
                clean_col = "col_" + clean_col
                
            cleaned_columns.append(clean_col)
        
        # Rename the columns in the dataframe
        column_mapping = {old: new for old, new in zip(df.columns, cleaned_columns)}
        df = df.rename(columns=column_mapping)
        
        # Prepare cursor
        cursor = connection.cursor()
        
        # Get column names
        columns = df.columns.tolist()
        columns_str = ", ".join(columns)
        
        # Get column info for validation
        cursor.execute(f"DESCRIBE {table_name}")
        table_columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Prepare the INSERT statement
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Insert row by row with validation
        total_rows = len(df)
        inserted_rows = 0
        errors = 0
        batch_size = 100  # Smaller batch size for better error handling
        
        for i, row in df.iterrows():
            try:
                # Replace NaN values with None for MySQL
                values = []
                for j, (col, val) in enumerate(zip(columns, row)):
                    if pd.isna(val):
                        values.append(None)
                    else:
                        # Handle data type conversions
                        col_type = table_columns.get(col, "").lower()
                        
                        # For numeric fields, enforce proper type casting
                        if "int" in col_type and not pd.isna(val):
                            try:
                                values.append(int(float(val)))
                            except (ValueError, TypeError):
                                values.append(None)  # Can't convert to int, use NULL
                        elif ("float" in col_type or "double" in col_type) and not pd.isna(val):
                            try:
                                values.append(float(val))
                            except (ValueError, TypeError):
                                values.append(None)  # Can't convert to float, use NULL
                        elif "varchar" in col_type or "text" in col_type:
                            # Convert to string and truncate if needed
                            if val is not None:
                                str_val = str(val)
                                # Extract size limit from varchar(X)
                                if "varchar" in col_type:
                                    size_match = re.search(r'varchar\((\d+)\)', col_type)
                                    if size_match:
                                        size_limit = int(size_match.group(1))
                                        if len(str_val) > size_limit:
                                            print(f"Warning: Value truncated for {col} at row {i+1}: '{str_val[:20]}...' ({len(str_val)} chars)")
                                values.append(str_val)
                            else:
                                values.append(None)
                        else:
                            values.append(val)
                
                cursor.execute(insert_query, tuple(values))
                inserted_rows += 1
                
                # Commit every batch_size rows
                if (i+1) % batch_size == 0:
                    connection.commit()
                    print(f"Inserted {i+1}/{total_rows} rows")
                
            except Error as e:
                errors += 1
                print(f"Error on row {i+1}: {e}")
                # Continue with next row instead of failing completely
                if errors >= 10:  # Limit number of errors before giving up
                    print(f"Too many errors ({errors}), aborting import")
                    connection.rollback()
                    return False
        
        # Final commit
        connection.commit()
        print(f"Successfully inserted {inserted_rows}/{total_rows} rows from CSV into MySQL table '{table_name}'")
        if errors > 0:
            print(f"There were {errors} rows with errors that couldn't be imported")
        return inserted_rows > 0
        
    except Error as e:
        print(f"Error inserting data: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
        
        # Insert row by row
        total_rows = len(df)
        for i, row in df.iterrows():
            # Replace NaN values with None for MySQL
            values = [None if pd.isna(val) else val for val in row]
            cursor.execute(insert_query, tuple(values))
            
            # Print progress
            if (i+1) % 1000 == 0 or i+1 == total_rows:
                print(f"Inserted {i+1}/{total_rows} rows")
                connection.commit()  # Commit every 1000 rows
        
        # Final commit if needed
        connection.commit()
        print(f"Successfully inserted all {total_rows} rows from CSV into MySQL table '{table_name}'")
        return True
        
    except Error as e:
        print(f"Error inserting data: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def main():
    # Database connection parameters
    host = "localhost"
    database = "world"
    user = "root"
    password = "root"
    
    # CSV file path
    csv_file_path = "world-happiness-report-2021.csv"
    
    # Table name
    table_name = "WorldHappiness"
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' not found.")
        sys.exit(1)
        
    # Connect to database
    connection = connect_to_mysql(host, database, user, password)
    if not connection:
        sys.exit(1)
    
    try:
        # Infer column types from CSV
        column_info = get_column_types_from_csv(csv_file_path)
        if not column_info:
            sys.exit(1)
            
        column_types, original_columns = column_info
            
        # Create table if it doesn't exist
        if not create_table(connection, table_name, column_types):
            sys.exit(1)
            
        # Insert data from CSV
        if not insert_csv_data(connection, csv_file_path, table_name):
            sys.exit(1)
            
        print("CSV import completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close connection
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    main()