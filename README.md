# database_orm
Database homework

Short and compact ETL pipeline for the Sakila Database, from MySQL to SQLite.

Uses SQLAlchemy and PyMySQL

Global variables:
MYSQL_URL - used to connect to your MYSQL, defaulted to be
mysql+pymysql://user:pass@localhost/sakila

SQLITE_URL - path to the SQLite file. defaulted to be sqlite:///sakila_analytics.db

To run:

to initiate the empty SQLite database, use (while in the directory folder)

python main.py init

then to move all data from MySQL Sakila over, use:

python main.py full-load

To sync changes made to MySQL since the last sync time:

python main.py incremental

To validate the two databases are in sync, use:

python main.py validate

for help, run python main.py -h


--------
Architecture
--------

We follow closely the architecture suggested in the prompt, namely having

Dimensions being customer, film, store, actor, category and date
Facts are rental and payment


---------
Testing

There is a (tiny) test suite to test each of the 5 functions of the CLI.
To run it, use (while in the repo directory):

pytest test_sync.py -v

