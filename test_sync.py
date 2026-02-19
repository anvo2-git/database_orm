import pytest
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.orm import sessionmaker
from connectors import MySQLSession, SQLiteSession
from models import SakilaBase, Rental
from models import FactRental
from datetime import datetime

from sync import init_command, run_full_load, run_sync, sync_fact_rental_inc, sync_fact_payment_inc, validate

def test_init_command():
    """Init"""
    init_command()
    engine = create_engine("sqlite:///sakila_analytics.db")
    inspector = inspect(engine)
    assert inspector.has_table("fact_rental"), f'Missing table fact_rental!'
    assert inspector.has_table("dim_customer"), 'Missing dim_customer!'
    assert inspector.has_table("sync_state"), 'Missting sync_state!'

def test_full_load():

    run_full_load()
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()
    mysql_count = mysql_session.query(func.count(Rental.rental_id)).scalar()
    sqlite_count = sqlite_session.query(func.count(FactRental.rental_id)).scalar()
    
    assert sqlite_count == mysql_count, f"Count mismatch -- MySQL:{mysql_count},SQLite: {sqlite_count}"
    assert sqlite_count > 0,"Full-loaded, but SQLite is empty"
    mysql_session.close()
    sqlite_session.close()
def test_incremental_new_data():
    """Increments!"""
    # Simulate a new rental in MySQL
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()
    new_rental = Rental(inventory_id=1, customer_id=1, staff_id=1, rental_date=datetime.now, last_update = datetime.now)
    try:
        mysql_session.add(new_rental)
        mysql_session.commit()
    
        run_sync()
        sqlite_session.expire_all()
        exists = sqlite_session.query(FactRental).filter_by(rental_id=new_rental.rental_id).first()

        assert exists is not None, 'Could not locate injected datapoint in SQLite'
    finally:
        mysql_session.close()
        sqlite_session.close()
