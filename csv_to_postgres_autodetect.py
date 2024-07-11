import settings
import csv
import time
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.types import TypeDecorator, VARCHAR, INTEGER, BIGINT, FLOAT, DATE, BOOLEAN


def format_time(raw_time):
    if raw_time > 60:
        return f"{(raw_time / 60):.2f} minutes"
    else:
        return f"{raw_time:.2f} seconds"


def get_data_type(value):
    if value is None or value == '':
        return None
    if value.isdigit():
        if len(value) <= 9:
            return INTEGER
        else:
            return BIGINT
    try:
        float(value)
        return FLOAT
    except ValueError:
        pass
    if value.lower() in ('true', 'false'):
        return LowerCaseBoolean
    try:
        datetime.strptime(value, '%m/%d/%Y %I:%M:%S %p')
        return DATE
    except ValueError:
        return VARCHAR


class LowerCaseBoolean(TypeDecorator):
    impl = BOOLEAN
    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.lower() == 'true'

    def process_result_value(self, value, dialect):
        return value


start_time = time.time()

# DB stuff
engine = create_engine(URL.create(**settings.DB_URL))
Session = sessionmaker(bind=engine)
Base = declarative_base()
table_name = settings.table_name

# Read the CSV file
with open(settings.input_file, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)

    # Create a table if the table doesn't already exist
    if not engine.dialect.has_table(engine.connect(), table_name):
        # Determine column data types based on first n rows of data
        data_types = {column: set() for column in headers}
        for i, row in enumerate(reader):
            if i >= settings.datatype_detection_sample_size:
                break
            for column, value in zip(headers, row):
                data_type = get_data_type(value)
                if data_type is not None:
                    data_types[column].add(data_type)

        # If multiple data types are detected, set to VARCHAR
        final_data_types = {}
        for column, types in data_types.items():
            if len(types) > 1:
                final_data_types[column] = VARCHAR
            elif len(types) == 1:
                final_data_types[column] = types.pop()
            else:
                final_data_types[column] = VARCHAR

        # Reset file read to beginning and skip the header
        f.seek(0)
        next(reader)

        # Create the table
        metadata = MetaData()
        table = Table(table_name,
                      metadata,
                      *(Column(column, data_type, nullable=True) for column, data_type in final_data_types.items()),
                      schema=settings.schema)

        print({column: str(final_data_types[column]) for column in headers})
        metadata.create_all(engine)
    else:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)


    # Insert the data into the table in batches
    batch_size = settings.batch_size
    session = Session()
    batch = []
    num_batches = 0
    for row in reader:
        # empty string check in the append to avoid sqlalchemy datatype issues when empty strings present in data
        batch.append({header: value if value != '' else None for header, value in zip(headers, row)})
        if len(batch) >= batch_size:
            session.execute(table.insert(), batch)
            batch = []
            num_batches += 1
            print(f"Wrote batch {num_batches} - {format_time(time.time() - start_time)} elapsed since start.")

    if batch:
        session.execute(table.insert(), batch)
        print(f"Final batch had {len(batch)} rows. \n{num_batches * batch_size + len(batch)} rows were inserted.")
    session.commit()
    session.close()

print(f"Total loading time:  {format_time(time.time() - start_time)}")

