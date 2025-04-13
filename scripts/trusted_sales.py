from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, TIMESTAMP, text, insert, select, cast, func
from sqlalchemy.dialects.postgresql import JSON
from config import db_host, db_user, db_password, db_port, db_name
import json

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}', echo=True)
metadata = MetaData()

with engine.connect() as connection:
    delta_ini = connection.execute(text("SELECT COALESCE(max(batch_id), 0) FROM trusted_sales")).scalar()
    insert_query = text("""
    INSERT INTO trusted_sales (id, first_name, last_name, email, gender, ip_address, value, discount, batch_id)
    SELECT 
        id,
        json_extract_path_text(data::JSON, 'first_name'),
        json_extract_path_text(data::JSON, 'last_name'),
        json_extract_path_text(data::JSON, 'email'),
        json_extract_path_text(data::JSON, 'gender'),
        json_extract_path_text(data::JSON, 'ip_address'),
        json_extract_path_text(data::JSON, 'value')::double precision,
        json_extract_path_text(data::JSON, 'discount')::double precision,
        batch_id
    FROM raw_sales
    WHERE batch_id > :delta_ini AND id IS NOT NULL 
    """)
    try:
        connection.execute(insert_query, {"delta_ini": delta_ini})
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()