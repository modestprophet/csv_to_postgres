import settings
import csv
import time
from sqlalchemy import create_engine, Table, MetaData, Column, String
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker


def format_time(raw_time):
    if raw_time > 60:
        return f"{(raw_time / 60):.2f} minutes"
    else:
        return f"{raw_time:.2f} seconds"


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

    # Create a table object
    if not engine.dialect.has_table(engine.connect(), table_name):
        metadata = MetaData()
        table = Table(table_name, metadata,
                      *(Column(header, String) for header in headers))
        metadata.create_all(engine)

    # Insert the data into the table in batches
    batch_size = settings.batch_size
    session = Session()
    batch = []
    num_batches = 0
    for row in reader:
        batch.append({header: value for header, value in zip(headers, row)})
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
