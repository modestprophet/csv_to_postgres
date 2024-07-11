# csv_to_postgres

Does what it says on the tin.  A simple script to load a csv file into postgres.  
Supports loading in batches for larger CSV files. 

### csv_to_postgres.py
Simpler and probably less likely to fail when a new table is required as all columns are str/varchar

### csv_to_postgres_autodetect.py
Tries to autodetect datatype for each column.  Use settings.datatype_detection_sample_size to increase/decrease the
amount of data used to detect a column's datatype.  Edge cases will make sqlalchemy very mad.  

Update settings.py with DB info along with other parameters you'd expect.  I use Hashicorp Vault for secrets management.
Feel free to comment out and modify the Vault related DB credential details to use env variables or hard code them in 
settings.py if you want to live dangerously.  

TODO:  Maybe add arg parsing for csv path, table name, schema name, and batch size