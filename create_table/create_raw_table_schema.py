from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float
from config import db_host, db_user, db_password, db_port, db_name

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}',  echo = True)
metadata = MetaData()

trusted_table = Table(
    'trusted_sales',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('first_name', String),
    Column('last_name', String),
    Column('email', String),
    Column('gender', String),
    Column('ip_address', String),
    Column('value', Float),
    Column('discount', Float)
)


metadata.create_all(engine)