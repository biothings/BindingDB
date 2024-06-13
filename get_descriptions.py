import sqlite3
import subprocess
import os

def execute_command(command):
    # Use subprocess.Popen to run the command
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=True
    )
    
    # Read stdout line-by-line
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            yield output.strip()
    
    # Capture and print any remaining stderr output
    stderr = process.communicate()[1]
    if stderr:
        print(stderr.strip())


def get_descriptions(data_folder):
    print("Loading descriptions from MySQL dump....")
    sql_file_path = os.path.join(data_folder, './BDB-mySQL_All_202406.dmp')
    awk_file_path = os.path.join(data_folder, './mysql2sqlite')

    # Connect to SQLite (or create the database if it doesn't exist)
    conn = sqlite3.connect(':memory:')  # You can change 'example.db' to your desired database name

    # Create a cursor object
    cursor = conn.cursor()

    # Read and execute the SQL file line-by-line
    sql_command = ""
    for line in execute_command(f"awk -f {awk_file_path} < {sql_file_path}"):
        # Skip comments and empty lines
        if line.strip().startswith('--') or not line.strip():
            continue
        
        # Accumulate SQL command
        sql_command += line.strip()
        
        # If the line ends with a semicolon, execute the command
        if sql_command.endswith(';'):
            try:
                cursor.execute(sql_command)
                sql_command = ""  # Reset the command after execution
            except Exception as e:
                sql_command = ""  # Reset the command even if there's an error

    # Commit the changes
    conn.commit()

    # Create a cursor object to execute queries
    cursor = conn.cursor()

    # Execute the query
    query = "select reactant_set_id, assayid, entryid from ki_result;"
    cursor.execute(query)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    mappings = {}

    # Process the rows
    for row in rows:
        mappings[row[0]] = (row[1], row[2])

    # Execute another query
    query = "select description, assayid, entryid from assay;"
    cursor.execute(query)

    mappings2 = {}

    rows = cursor.fetchall()

    for row in rows:
        if type(row[0]) is str:
            mappings2[f"{row[1]}-{row[2]}"] = row[0]

    final_mappings = {}
    for (k, v) in mappings.items():
        try:
            final_mappings[str(k)] = mappings2[f"{v[0]}-{v[1]}"]
        except KeyError:
            final_mappings[str(k)] = None
    # Close the cursor and connection
    cursor.close()

    # Close the connection
    conn.close()

    print("Done loading descriptions from MySQL dump.")
    return final_mappings