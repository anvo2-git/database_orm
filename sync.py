


import sys
from datetime import date, timedelta, datetime
from sqlalchemy.orm import joinedload
from sqlalchemy import text, func, or_
from connectors import mysql_engine, sqlite_engine, SQLiteSession, MySQLSession
from models import LiteBase, DimDate, SyncState
from models import (Actor, DimActor, Film, DimFilm, Language)
from models import (Customer, Address, City, Country, DimCustomer)
from models import (Store, Category, DimStore, DimCategory)
from models import (FilmActor, FilmCategory, BridgeFilmActor, BridgeFilmCategory)
from models import (Rental, Inventory, Payment, FactRental, FactPayment, Staff)
import argparse

def verify_mysql_connection():
    """Checks for MySQL"""
    try:
        with mysql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Check succeeded. Connected to MYSQL")
        return True
    except Exception as e:
        print(f"Could not connect to MYSQL {e}")
        return False

#INIT FUNCTIONS
def populate_dim_date(session, start_year=1987, end_year=2035):
    """Generates and inserts date records into dim_date."""

    if session.query(DimDate).first():
        print("dim_date already exists")
        return

    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    delta = timedelta(days=1)
    current_date = start_date
    date_records = []
    
    print(f"Populating dim_date between {start_year} and {end_year}")
    
    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d')) 
        
        #Datetime calculation
        day_of_week = current_date.isoweekday()
        is_weekend = 1 if day_of_week in [6, 7] else 0
        quarter = (current_date.month - 1) // 3 + 1
        
        dim_date_record = DimDate(
            date_key=date_key,
            date=current_date.strftime('%Y-%m-%d'),
            year=current_date.year,
            quarter=quarter,
            month=current_date.month,
            day_of_month=current_date.day,
            day_of_week=day_of_week,
            is_weekend=is_weekend
        )
        date_records.append(dim_date_record)
        current_date += delta
        
    session.bulk_save_objects(date_records)
    print(f"Successfully staged {len(date_records)} dates for dim_date.")

def init_sync_state(session):
    """Initializes the sync_state table with default old timestamps."""

    tables_to_sync = [
        'film', 'actor', 'category', 'store', 'customer', 
        'film_actor', 'film_category', 'rental', 'payment'
    ]
    
    #Dummy date to spark the sync process
    default_old_date = datetime(1970, 1, 1)
    
    for table in tables_to_sync:
        exists = session.query(SyncState).filter_by(table_name=table).first()
        if not exists:
            state = SyncState(table_name=table, last_sync_timestamp=default_old_date)
            session.add(state)
            
    print("Sync_state populated")

#FULL LOAD FUNCTIONS
def load_dims(mysql_session, sqlite_session):
    """Gets information from Dims. Includes Actors, Films, Customers,
    Stores, and Categories."""
    print("Getting Actors from Sakila")
    sakila_a = mysql_session.query(Actor).all()
    
    dim_actors = []
    for actor in sakila_a:
        dim_actors.append(DimActor(
            actor_id=actor.actor_id,
            first_name=actor.first_name,
            last_name=actor.last_name,
            last_update=str(actor.last_update)
        ))
    
    sqlite_session.bulk_save_objects(dim_actors)
    print(f"Loaded {len(dim_actors)} records into dim_actor.")

    print('Moving on...')
    print("Getting dim Films from Sakila")
    sakila_f = (
        mysql_session.query(Film, Language.name)
        .join(Language, Film.language_id == Language.language_id)
        .all()
    )
    dim_films = []
    for film, language_name in sakila_f:
        dim_films.append(DimFilm(
            film_id=film.film_id,
            title=film.title,
            release_year=film.release_year,
            language=language_name,
            rating=film.rating,
            length=film.length,
            last_update=str(film.last_update) 
        ))
        
    sqlite_session.bulk_save_objects(dim_films)
    print(f"Loaded {len(dim_films)} records into dim_film.")
    print('Moving on...')
    print("Getting dim Customers from Sakila, joining with Address, City, Country")

    sakila_c = (
        mysql_session.query(Customer, City.city, Country.country)
        .join(Address, Customer.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
        .all()
    )
    
    dim_customers = []
    for customer, city_name, country_name in sakila_c:
        dim_customers.append(DimCustomer(
            customer_id=customer.customer_id,
            first_name=customer.first_name,
            last_name=customer.last_name,
            active=1 if customer.active else 0, 
            city=city_name,
            country=country_name,
            last_update=str(customer.last_update)
        ))
        
    sqlite_session.bulk_save_objects(dim_customers)
    print(f"Loaded {len(dim_customers)} records into dim_customer.")
    print('Moving on...')
    print("Getting dim Stores from Sakila, joining with Address, City, Country")

    sakila_s = (
        mysql_session.query(Store, City.city, Country.country)
        .join(Address, Store.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
        .all()
    )
    
    dim_stores = []
    
    for store, city_name, country_name in sakila_s:
        dim_stores.append(DimStore(
            store_id=store.store_id,
            city=city_name,
            country=country_name,
            last_update=str(store.last_update)
        ))
        
    sqlite_session.bulk_save_objects(dim_stores)
    print(f"Loaded {len(dim_stores)} records into dim_store.")
    print('Last one. Phew!')
    print("Getting Categories from Sakila, joined with nothing!")
    
    source_categories = mysql_session.query(Category).all()
    
    dim_categories = []
    for category in source_categories:
        dim_categories.append(DimCategory(
            category_id=category.category_id,
            name=category.name,
            last_update=str(category.last_update)
        ))
        
    sqlite_session.bulk_save_objects(dim_categories)
    print(f"Loaded {len(dim_categories)} records into dim_category.")

    sqlite_session.flush() 

def load_bridges(mysql_session, sqlite_session):
    """Gets the junction tables from MySQL and move them to SQLite."""
    print("Getting SQLITE's keys")
    
    #Move from Sakila's ID -> SQLite's ID
    map_f = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm.film_id, DimFilm.film_key).all()}
    map_a = {a.actor_id: a.actor_key for a in sqlite_session.query(DimActor.actor_id, DimActor.actor_key).all()}
    map_c = {c.category_id: c.category_key for c in sqlite_session.query(DimCategory.category_id, DimCategory.category_key).all()}


    print("Scraping Film-Actor from Sakila...")
    sakila_f_a = mysql_session.query(FilmActor).all()
    
    bridge_film_actors = []
    for mapping in sakila_f_a:
        # Translate the old IDs into the new Keys
        f_key = map_f.get(mapping.film_id)
        a_key = map_a.get(mapping.actor_id)

        if f_key and a_key:
            bridge_film_actors.append(BridgeFilmActor(
                film_key=f_key,
                actor_key=a_key
            ))
            
    sqlite_session.bulk_save_objects(bridge_film_actors)
    print(f"Loaded {len(bridge_film_actors)} records into bridge_film_actor.")

    print('Moving on...')
    print("Scraping Film-Category from Sakila...")
    sakila_f_c = mysql_session.query(FilmCategory).all()
    
    bridge_film_categories = []
    for mapping in sakila_f_c:
        f_key = map_f.get(mapping.film_id)
        c_key = map_c.get(mapping.category_id)
        
        if f_key and c_key:
            bridge_film_categories.append(BridgeFilmCategory(
                film_key=f_key,
                category_key=c_key
            ))
            
    sqlite_session.bulk_save_objects(bridge_film_categories)
    print(f"Loaded {len(bridge_film_categories)} records into bridge_film_category.")

    sqlite_session.flush()

def load_facts(mysql_session, sqlite_session):
    """Gets the transactions from MySQL and populates them in SQLite with the appropriate keys."""
    
    #hashmap SQLite keys so we avoid joining
    map_c = {c.customer_id: c.customer_key for c in sqlite_session.query(DimCustomer.customer_id, DimCustomer.customer_key).all()}
    map_s = {s.store_id: s.store_key for s in sqlite_session.query(DimStore.store_id, DimStore.store_key).all()}
    map_f = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm.film_id, DimFilm.film_key).all()}
    map_i = {inv.inventory_id: {'film_id': inv.film_id, 'store_id': inv.store_id} for inv in mysql_session.query(Inventory).all()}
    map_st = {staff.staff_id: staff.store_id for staff in mysql_session.query(Staff).all()}
    #Payment is a little more involved since we'll need to add them up
    payments = mysql_session.query(Payment).all()
    map_p = {}
    for p in payments:
        if p.rental_id:
            #Add up those with multiple payments
            map_p[p.rental_id] = map_p.get(p.rental_id, 0) + float(p.amount)

    print('Got everything we need to fill in Rentals')
    print("Extracting Rentals and building Fact table...")
    sakila_r = mysql_session.query(Rental).all()
    
    fact_rentals = []
    for rental in sakila_r:
        # Transform the datetime into our YYYYMMDD integer date_key
        rental_date_key = int(rental.rental_date.strftime('%Y%m%d'))
        returned_key = None
        if rental.return_date:
            returned_key = int(rental.return_date.strftime('%Y%m%d'))
        # Look up the source IDs from the inventory map
        inv_data = map_i.get(rental.inventory_id, {})
        source_film_id = inv_data.get('film_id')
        source_store_id = inv_data.get('store_id')
        
        # Translate source IDs to our new SQLite Surrogate Keys
        c_key = map_c.get(rental.customer_id)
        f_key = map_f.get(source_film_id)
        s_key = map_s.get(source_store_id)
        
        
        # Only build the fact record if we successfully found all our dimension keys
        if c_key and f_key and s_key:
            fact_rentals.append(FactRental(
                rental_id=rental.rental_id,
                date_key_rented=rental_date_key,
                date_key_returned=returned_key,
                customer_key=c_key,
                film_key=f_key,
                store_key=s_key,
                last_update=str(rental.last_update)
            ))
            
    sqlite_session.bulk_save_objects(fact_rentals)
    print(f"Loaded {len(fact_rentals)} records into fact_rental.")
    print('Moving on to FactPayment')
    print("Building FactPayment table...")
    fact_payments = []
    
    for p in payments:
        p_date_key = int(p.payment_date.strftime('%Y%m%d'))
        
        # staff_id -> store_id -> store_key
        source_store_id = map_st.get(p.staff_id)
        c_key = map_c.get(p.customer_id)
        s_key = map_s.get(source_store_id)
        
        if c_key and s_key:
            fact_payments.append(FactPayment(
                payment_id=p.payment_id,
                date_key_paid=p_date_key,
                rental_id = p.rental_id,
                customer_key=c_key,
                store_key=s_key,
                amount=float(p.amount),
                last_update=str(p.last_update)
            ))
            
    sqlite_session.bulk_save_objects(fact_payments)
    print(f"Loaded {len(fact_payments)} records into fact_payment.")

def run_full_load():
    """Main execution function for the 'Full-load' command."""
    print("Starting Full Load Process...")
    
    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()
    
    try:
        row_count = sqlite_session.query(FactRental).count()
        if row_count > 0:
            print(f"SQLite already contains {row_count} records.")
            print("Use 'incremental' to sync new data, or 'init' to start over.")
            return
        load_dims(mysql_session, sqlite_session)
        load_bridges(mysql_session, sqlite_session)
        load_facts(mysql_session, sqlite_session)
        sqlite_session.commit()
        print("Full Load SUCCESSFUL.")
        
    except Exception as e:
        sqlite_session.rollback()
        print(f"Full Load FAILED. Transaction rolled back. Error: {e}")
    finally:
        mysql_session.close()
        sqlite_session.close()

#INCREMENTAL
#HELPERS
def get_last_sync(sqlite_session, table_name):
    """Gets the last sync timestamp by querying SyncState. If doesn't exist,
    output an old datetime"""
    state = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
    return state.last_sync_timestamp if state else datetime(1970, 1, 1)

def update_sync_state(sqlite_session, table_name, max_ts):
    """Scrapes the most recent timestamp from MySQL, then records it in SQLLite """
    if max_ts:
        state = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
        if not state:
            state = SyncState(table_name=table_name)
            sqlite_session.add(state)
        state.last_sync_timestamp = max_ts

def upsert_dimension(sqlite_session, target_model, mysql_key_name, data_list):
    '''Handles upserting of a SQLite row
    '''
    for item_dict in data_list:
        b_key_val = item_dict.get(mysql_key_name)
        #Check if record exists in SQLite
        existing = sqlite_session.query(target_model).filter(
            getattr(target_model, mysql_key_name) == b_key_val
        ).first()

        if existing:
            # Update columns
            for key, value in item_dict.items():
                setattr(existing, key, value)
        else:
            # Insert new row
            sqlite_session.add(target_model(**item_dict))

def sync_dim_actor_inc(mysql_session, sqlite_session):
    ''' Syncs actor. This requires no joins.
    '''
    last_sync = get_last_sync(sqlite_session, 'dim_actor')
    changes = mysql_session.query(Actor).filter(Actor.last_update > last_sync).all()
    if changes:
        data = [{
            "actor_id": a.actor_id,
            "first_name": a.first_name,
            "last_name": a.last_name,
            "last_update": str(a.last_update)
        } for a in changes]
        upsert_dimension(sqlite_session, DimActor, 'actor_id', data)
        update_sync_state(sqlite_session, 'dim_actor', max(a.last_update for a in changes))
    return len(changes)

def sync_dim_category_inc(mysql_session, sqlite_session):
    '''Syncs categories. Also no joins!
    '''
    last_sync = get_last_sync(sqlite_session, 'dim_category')
    changes = mysql_session.query(Category).filter(Category.last_update > last_sync).all()

    if changes:
        data = [{
            "category_id": c.category_id,
            "name": c.name,
            "last_update": str(c.last_update)
        } for c in changes]
        
        upsert_dimension(sqlite_session, DimCategory, 'category_id', data)
        update_sync_state(sqlite_session, 'dim_category', max(c.last_update for c in changes))
    return len(changes)

#From here are dims that need joins
def sync_dim_store_inc(mysql_session, sqlite_session):
    '''Syncs the store dimension. Here we have to join with Address, City and Country
    '''
    last_sync = get_last_sync(sqlite_session, 'dim_store')
    
    #Flatten the join
    changes = mysql_session.query(Store).join(Address).join(City).join(Country).filter(
        or_(
            Store.last_update > last_sync,
            Address.last_update > last_sync
        )
    ).all()

    if changes:
        data = [{
            "store_id": s.store_id,
            "address": s.address.address,
            "city": s.address.city.city,
            "country": s.address.city.country.country,
            "last_update": str(s.last_update)
        } for s in changes]
        
        upsert_dimension(sqlite_session, DimStore, 'store_id', data)
        update_sync_state(sqlite_session, 'dim_store', max(s.last_update for s in changes))
    return len(changes)

def sync_dim_customer_inc(mysql_session, sqlite_session):
    '''Syncs customer. We'll need to join with Address, City, Country
    '''
    last_sync = get_last_sync(sqlite_session, 'dim_customer')
    changes = mysql_session.query(Customer).join(Address).join(City).join(Country).filter(
        or_(
            Customer.last_update > last_sync,
            Address.last_update > last_sync,
            City.last_update > last_sync
        )
    ).all()

    if changes:
        data = [{
            "customer_id": c.customer_id,
            "first_name": c.first_name,
            "last_name": c.last_name,
            "email": c.email,
            "address": c.address.address,
            "city": c.address.city.city,
            "country": c.address.city.country.country,
            "last_update": str(c.last_update)
        } for c in changes]
        
        upsert_dimension(sqlite_session, DimCustomer, 'customer_id', data)
        max_ts = max(c.last_update for c in changes)
        update_sync_state(sqlite_session, 'dim_customer', max_ts)
    return len(changes)

def sync_dim_film_inc(mysql_session, sqlite_session):
    '''Film syncs. Will need to be joined with Langauge'''
    last_sync = get_last_sync(sqlite_session, 'dim_film')
    changes = mysql_session.query(Film).join(Language, Film.language_id == Language.language_id).filter(
        or_(
            Film.last_update > last_sync,
            Language.last_update > last_sync
        )
    ).all()

    if changes:
        data = [{
            "film_id": f.film_id,
            "title": f.title,
            "release_year": f.release_year,
            "language": f.language.name, 
            "length": f.length,
            "rating": f.rating,
            "last_update": str(f.last_update)
        } for f in changes]
        
        upsert_dimension(sqlite_session, DimFilm, 'film_id', data)
        max_ts = max(f.last_update for f in changes)
        update_sync_state(sqlite_session, 'dim_film', max_ts)     
    return len(changes)

#Bridge tables
def sync_bridge_film_actor_inc(mysql_session, sqlite_session):
    '''Syncs bridge tables. We'll need to delete, then scrape, then re-insert'''
    last_sync = get_last_sync(sqlite_session, 'bridge_film_actor')
    changed_film_ids = [f.film_id for f in mysql_session.query(Film.film_id).filter(Film.last_update > last_sync).all()]

    if not changed_film_ids:
        return 0

    film_map = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm).filter(DimFilm.film_id.in_(changed_film_ids)).all()}
    actor_map = {a.actor_id: a.actor_key for a in sqlite_session.query(DimActor).all()}
    #Delete
    sqlite_session.query(BridgeFilmActor).filter(BridgeFilmActor.film_key.in_(film_map.values())).delete(synchronize_session=False)

    changes = mysql_session.query(FilmActor).filter(FilmActor.film_id.in_(changed_film_ids)).all()
    for row in changes:
        new_entry = BridgeFilmActor(
            film_key=film_map.get(row.film_id),
            actor_key=actor_map.get(row.actor_id)
        )
        sqlite_session.add(new_entry)
    max_ts = mysql_session.query(func.max(FilmActor.last_update)).scalar()
    update_sync_state(sqlite_session, 'bridge_film_actor', max_ts)
    return len(changes)

def sync_bridge_film_category_inc(mysql_session, sqlite_session):
    '''
    Film_cat'''
    last_sync = get_last_sync(sqlite_session, 'bridge_film_category')
    
    changed_films = mysql_session.query(Film.film_id).filter(Film.last_update > last_sync).all()
    changed_film_ids = [f.film_id for f in changed_films]

    if not changed_film_ids:
        return 0
    #Same as above
    film_map = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm).filter(DimFilm.film_id.in_(changed_film_ids)).all()}
    category_map = {c.category_id: c.category_key for c in sqlite_session.query(DimCategory).all()}

    sqlite_session.query(BridgeFilmCategory).filter(
        BridgeFilmCategory.film_key.in_(film_map.values())
    ).delete(synchronize_session=False)

    assignments = mysql_session.query(FilmCategory).filter(FilmCategory.film_id.in_(changed_film_ids)).all()
    for row in assignments:
        new_entry = BridgeFilmCategory(
            film_key=film_map.get(row.film_id),
            category_key=category_map.get(row.category_id)
        )
        sqlite_session.add(new_entry)

    max_ts = mysql_session.query(func.max(FilmCategory.last_update)).scalar()
    update_sync_state(sqlite_session, 'bridge_film_category', max_ts)
    return len(assignments)

#Facts tables

def sync_fact_payment_inc(mysql_session, sqlite_session):
    '''Payment. Just need to get keys for customer
    '''
    last_sync = get_last_sync(sqlite_session, 'fact_payment')
    changes = mysql_session.query(Payment).filter(Payment.last_update > last_sync).all()
    if not changes: return 0

    payment_ids = [p.payment_id for p in changes]
    sqlite_session.query(FactPayment).filter(FactPayment.payment_id.in_(payment_ids)).delete(synchronize_session=False)
    cust_map = {c.customer_id: c.customer_key for c in sqlite_session.query(DimCustomer).all()}
    rental_store_map = {r.rental_id: r.store_key for r in sqlite_session.query(FactRental.rental_id, FactRental.store_key).all()}
    staff_map = {s.staff_id: s.store_id for s in mysql_session.query(Staff).all()}
    map_s = {s.store_id: s.store_key for s in sqlite_session.query(DimStore).all()}
    for p in changes:
        date_key = int(p.payment_date.strftime('%Y%m%d'))
        s_key = rental_store_map.get(p.rental_id)
        if s_key is None:
            source_store_id = staff_map.get(p.staff_id)
            s_key = map_s.get(source_store_id)
        sqlite_session.add(FactPayment(
            payment_id=p.payment_id,
            rental_id=p.rental_id,
            customer_key=cust_map.get(p.customer_id),
            store_key=s_key,
            staff_id=p.staff_id, #Note that we do NOT have a DimStaff!
            amount=float(p.amount),
            date_key_paid=date_key,
        ))
    update_sync_state(sqlite_session, 'fact_payment', max(p.last_update for p in changes))
    return len(changes)

def sync_fact_rental_inc(mysql_session, sqlite_session):
    '''The worst one of them all. This is so many joins.'''
    last_sync = get_last_sync(sqlite_session, 'fact_rental')
    changes = mysql_session.query(Rental)\
        .options(joinedload(Rental.inventory))\
        .filter(Rental.last_update > last_sync).all()
    if not changes: return 0
    rental_ids = [r.rental_id for r in changes]
    sqlite_session.query(FactRental).filter(FactRental.rental_id.in_(rental_ids)).delete(synchronize_session=False)

    cust_map = {c.customer_id: c.customer_key for c in sqlite_session.query(DimCustomer).all()}
    film_map = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm).all()}
    store_map = {st.store_id: st.store_key for st in sqlite_session.query(DimStore).all()}

    for r in changes:
        duration = None
        rented_key = int(r.rental_date.strftime('%Y%m%d'))
        return_key = int(r.return_date.strftime('%Y%m%d')) if r.return_date else None
        if r.return_date and r.rental_date:
            duration = (r.return_date - r.rental_date).total_seconds() / (3600*24)

        inv = r.inventory
        f_key = film_map.get(inv.film_id) if inv else None
        s_key = store_map.get(inv.store_id) if inv else None

        sqlite_session.add(FactRental(
            rental_id=r.rental_id,
            date_key_rented=rented_key,
            date_key_returned=return_key if return_key else None,
            customer_key=cust_map.get(r.customer_id),
            film_key=f_key,
            store_key=s_key,
            staff_id=r.staff_id, #Again, no dim_staff here
            rental_duration_days=duration
        ))

    update_sync_state(sqlite_session, 'fact_rental', max(r.last_update for r in changes))
    return len(changes)

def validate(mysql_session, sqlite_session):
    """Compares row counts and totals per store.
    """
    print("Validating")
    
    # mysql_session = MySQLSession()
    # sqlite_session = SQLiteSession()
    mysql_rentals = mysql_session.query(func.count(Rental.rental_id)).scalar()
    sqlite_rentals = sqlite_session.query(func.count(FactRental.rental_id)).scalar()
    
    mysql_store_totals = mysql_session.query(
        Inventory.store_id, 
        func.sum(Payment.amount)
    ).join(Rental, Payment.rental_id == Rental.rental_id)\
     .join(Inventory, Rental.inventory_id == Inventory.inventory_id)\
     .group_by(Inventory.store_id).all()

    sqlite_store_totals = sqlite_session.query(
        DimStore.store_id,
        func.sum(FactPayment.amount)
    ).join(FactRental, FactRental.store_key == DimStore.store_key)\
     .join(FactPayment, FactPayment.rental_id == FactRental.rental_id)\
     .group_by(DimStore.store_id).all()

    #Convert results for comparison: {id: total_amount}
    m_totals = {row[0]: round(float(row[1]), 2) for row in mysql_store_totals}
    s_totals = {row[0]: round(float(row[1]), 2) for row in sqlite_store_totals}

    is_valid = True
    
    #Check against record
    if mysql_rentals != sqlite_rentals:
        print(f" Wrong Total! MySQL: {mysql_rentals}, SQLite: {sqlite_rentals}")
        is_valid = False
    else:
        print(f" Total was {sqlite_rentals}")

    #Check per store as requested
    for store_id, m_amt in m_totals.items():
        s_amt = s_totals.get(store_id, 0)
        if m_amt != s_amt:
            print(f"Store {store_id} total mismatch! MySQL: ${m_amt}, SQLite: ${s_amt}")
            is_valid = False
        else:
            print(f"Store {store_id} Totals match! : ${s_amt}")

    return is_valid


def init_command():
    """Init!"""
    print("Starting initilisation")
    
    if not verify_mysql_connection():
        sys.exit(1) 
        
    print("Creating SQLite tables")
    LiteBase.metadata.create_all(sqlite_engine)
    

    session = SQLiteSession()
    try:
        populate_dim_date(session)
        init_sync_state(session)
        session.commit() 
        print("Initialization SUCCESSFUL. Database is ready for data loading.")
    except Exception as e:
        session.rollback() 
        print(f"Initialization FAILED. Transaction rolled back. Error: {e}")
    finally:
        session.close()

def run_sync(mysql_session, sqlite_session):
    """Big sync function to handle incremental syncing correctly and in order
    """
    print(f"Synching!!!! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
    
    # mysql_session = MySQLSession()
    # sqlite_session = SQLiteSession()
    try:
        print("\nSyncing Actor...")
        sync_dim_actor_inc(mysql_session, sqlite_session)
        print('Category')
        sync_dim_category_inc(mysql_session, sqlite_session)
        print('Store')
        sync_dim_store_inc(mysql_session, sqlite_session)
        print('Customer')
        sync_dim_customer_inc(mysql_session, sqlite_session)
        print('Film')
        sync_dim_film_inc(mysql_session, sqlite_session)

        print("\nNext, Syncing Bridge. Film Actor.")
        sync_bridge_film_actor_inc(mysql_session, sqlite_session)
        print('Film Category.')
        sync_bridge_film_category_inc(mysql_session, sqlite_session)

        print("\nLast and certainly not least, Syncing Facts. Rentals.")
        rentals_synced = sync_fact_rental_inc(mysql_session, sqlite_session)
        print('Payments.')
        payments_synced = sync_fact_payment_inc(mysql_session, sqlite_session)
        
        print(f"Processed {rentals_synced} rentals and {payments_synced} payments.")
        sqlite_session.flush()
        if validate(mysql_session, sqlite_session):
            sqlite_session.commit()
            print("Validation complete. Transaction committed.")
        else:
            sqlite_session.rollback()
            print("Inconsistency detected. Transaction rollbacked")

    except Exception as e:
        sqlite_session.rollback()
        print(f"Error: {str(e)}. Transaction rollback.")
    finally:
        mysql_session.close()
        sqlite_session.close()

def main():
    parser = argparse.ArgumentParser(description="Sakila SQLite Incremental Manager")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    #Init Command
    subparsers.add_parser('init', help='Initialise the SQLite database.')

    #Full-load Command
    subparsers.add_parser('full-load', help='Scrape from Sakila into SQLite.')

    #Incremental Command
    subparsers.add_parser('incremental', help='Load only new or changed data since the last sync.')

    #Validate Command
    subparsers.add_parser('validate', help='Verify data consistency.')

    args = parser.parse_args()

    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        if args.command == 'init':
            init_command()
            print("Init Success")

        elif args.command == 'full-load':
            run_full_load()
            print("Full load success")

        elif args.command == 'incremental':
            run_sync(mysql_session, sqlite_session)
            print("Successfully synced changes since last timestamp")

        elif args.command == 'validate':
            if validate(mysql_session, sqlite_session):
                print("Validation success.")
            else:
                print("Failure: Inconsistency detected between MySQL and SQLite.")
                sys.exit(1)

        else:
            parser.print_help()

    except Exception as e:
        print(f"ERROR!!!! {args.command}: {e}")
        sys.exit(1)
    finally:
        mysql_session.close()
        sqlite_session.close()

if __name__ == "__main__":
    main()