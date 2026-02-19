import pytest
from sqlalchemy import create_engine, func, inspect, text
from connectors import MySQLSession, SQLiteSession
from models import SakilaBase, Rental, Payment
from models import FactRental, FactPayment
from datetime import datetime, timedelta

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
    #Simulate a new rental in MySQL
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        test_time = datetime.now() + timedelta(minutes=3)
        template = mysql_session.query(Rental).first()
        new_rental = Rental(
            inventory_id=template.inventory_id, 
            customer_id=template.customer_id, 
            staff_id=template.staff_id, 
            rental_date=test_time, 
            last_update=test_time
        )
        mysql_session.add(new_rental)
        mysql_session.commit()
        sqlite_session.execute(text("DELETE FROM sync_state")) 
        sqlite_session.commit()
        run_sync(mysql_session, sqlite_session)
        sqlite_session.close()
        sqlite_session = SQLiteSession()
        exists = sqlite_session.query(FactRental).filter_by(rental_id=new_rental.rental_id).first()

        assert exists is not None, 'Could not locate injected datapoint in SQLite'
    finally:
        mysql_session.close()
        sqlite_session.close()

from datetime import datetime
from sqlalchemy import text

def test_incremental_payment():
    """Multiple injections for payment"""
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        template = mysql_session.query(Payment).first()
        test_time = datetime.now()
        
        #Inject 3 new payments into MySQL
        new_payments = [
            Payment(
                customer_id=template.customer_id,
                staff_id=template.staff_id,
                rental_id=template.rental_id,
                amount=11.11,
                payment_date=test_time,
                last_update=test_time
            ),
            Payment(
                customer_id=template.customer_id,
                staff_id=template.staff_id,
                rental_id=template.rental_id,
                amount=33.77,
                payment_date=test_time,
                last_update=test_time
            ),
            Payment(
                customer_id=template.customer_id,
                staff_id=template.staff_id,
                rental_id=template.rental_id,
                amount=66.23,
                payment_date=test_time,
                last_update=test_time
            )
        ]
        
        mysql_session.add_all(new_payments)
        mysql_session.commit()
        sqlite_session.execute(text("DELETE FROM sync_state WHERE table_name='fact_payment'"))
        sqlite_session.commit()
        sync_fact_payment_inc(mysql_session, sqlite_session)
        sqlite_session.commit()

        sqlite_session.close()
        sqlite_session = SQLiteSession()
        
        for p in new_payments:
            exists = sqlite_session.query(FactPayment).filter_by(payment_id=p.payment_id).first()
            assert exists is not None, f"Payment ID {p.payment_id} failed to sync."
            assert float(exists.amount) == float(p.amount)
            expected_date_key = int(test_time.strftime('%Y%m%d'))
            assert exists.date_key_paid == expected_date_key

    finally:
        mysql_session.close()
        sqlite_session.close()
    
from datetime import datetime
import time

def test_updates():
    """"""
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        #Pick an existing rental that is already synced
        rental = mysql_session.query(Rental).first()
        rental_id = rental.rental_id
        
        new_return_date = datetime(2026, 1, 1, 10, 0, 0)
        new_timestamp = datetime.now()     
        rental.return_date = new_return_date
        rental.last_update = new_timestamp
        mysql_session.commit()
        run_sync(mysql_session, sqlite_session)
        
        sqlite_session.expire_all()
        updated_row = sqlite_session.query(FactRental).filter_by(rental_id=rental_id).first()
        
        # Verify the date_key_returned was updated correctly
        expected_key = int(new_return_date.strftime('%Y%m%d'))
        
        assert updated_row is not None, "Deleted instead of updating. This is super bad!"
        assert updated_row.date_key_returned == expected_key, \
            f"Update failed. Expected {expected_key}, got {updated_row.date_key_returned}"
            
    finally:
        mysql_session.close()
        sqlite_session.close()

def test_validate():
    """Test Validate"""
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        run_sync(mysql_session, sqlite_session)
        result = validate(mysql_session, sqlite_session)

        assert result is True, "Validation failed!"
        
    finally:
        mysql_session.close()
        sqlite_session.close()