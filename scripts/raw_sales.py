import psycopg2
import config
import json
import os
import re

def create_connection(host, port, user, password, dbname):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    )
    return conn

def create_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS raw_sales ("
                "id INTEGER PRIMARY KEY,"
                "data TEXT)")
    conn.commit()
    cur.close()

def delta_ini(conn):
    cur = conn.cursor()
    cur.execute("SELECT max(id) FROM raw_sales")
    max_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return(max_id)

def order_batch_numbers(folder_path):
    batch_numbers = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json') and 'batch_' in file_name:
            # Extract the numeric part of the file name
            match = re.search(r'batch_(\d+)', file_name)
            batch_numbers.append(int(match.group(1)))
        batch_numbers.sort()
    return batch_numbers
    
def insert_data(conn):
    cur = conn.cursor()
    folder_path = "/opt/airflow/data"
    
    cur.execute("SELECT coalesce(max(batch_id), 0) AS max_batch FROM raw_sales")
    ini_delta = cur.fetchone()[0]

    batch_numbers = order_batch_numbers(folder_path)
    for i in batch_numbers:
        if ini_delta is not None and i > ini_delta:
            print(os.listdir(folder_path))
            data = json.load(open(folder_path + "/batch_{}.json".format(i), 'r'))
            for j in range(len(data)):
                pk = data[j]['id']
                data[j].pop('id')
                dump = json.dumps(data[j])
                cur.execute("INSERT INTO raw_sales (id, data, batch_id) VALUES (%s, %s, %s)", (pk, dump, i))
            print(f"Data from batch_{i} loaded")
        else:
            print("No new data loaded")
            conn.commit()
            cur.close()
            return False
    conn.commit()
    cur.close()


conn = create_connection(config.db_host, config.db_port, config.db_user, config.db_password, config.db_name)

# create_table(conn)
insert_data(conn)


