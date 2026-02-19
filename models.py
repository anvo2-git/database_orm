from sqlalchemy import Index, Column, Integer, String, DateTime, ForeignKey, Numeric, Boolean, SmallInteger, Float
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

LiteBase = declarative_base()
SakilaBase = declarative_base()
#Dimendions
class DimDate(LiteBase):
    __tablename__ = 'dim_date'
    
    date_key = Column(Integer, primary_key=True) 
    date = Column(String) #SQL-Lite, so we keep things lite
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer)
    is_weekend = Column(Integer)

class DimFilm(LiteBase):
    __tablename__ = 'dim_film'
    
    film_key = Column(Integer,primary_key=True,autoincrement=True)
    film_id = Column(Integer) #From Sakila
    title = Column(String)
    rating = Column(String)
    length = Column(Integer)
    language = Column(String)
    release_year = Column(Integer)
    last_update = Column(String)
    __table_args__ = (Index('idx_film_key', 'film_id'),)
class DimActor(LiteBase):
    __tablename__ = 'dim_actor'
    
    actor_key = Column(Integer,primary_key=True,autoincrement=True)
    actor_id = Column(Integer) #Same as above
    first_name = Column(String)
    last_name = Column(String)
    last_update = Column(String)
    __table_args__ = (Index('idx_actor_key', 'actor_id'),)
class DimCategory(LiteBase):
    __tablename__ = 'dim_category'
    
    category_key = Column(Integer,primary_key=True,autoincrement=True)
    category_id = Column(Integer)
    name = Column(String)
    last_update = Column(String)
    __table_args__ = (Index('idx_category_key', 'category_id'),)
class DimStore(LiteBase):
    __tablename__ = 'dim_store'
    
    store_key = Column(Integer,primary_key=True,autoincrement=True)
    store_id = Column(Integer) # Natural key
    city = Column(String)
    country = Column(String)
    last_update = Column(String)
    __table_args__ = (Index('idx_store_key', 'store_id'),)

class DimCustomer(LiteBase):
    __tablename__ = 'dim_customer'
    
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer)
    first_name = Column(String)
    last_name = Column(String)
    active = Column(Integer) #Boolean
    city = Column(String)
    country = Column(String)
    last_update = Column(String)
    __table_args__ = (Index('idx_customer_key', 'customer_id'),)

#Bridges

class BridgeFilmActor(LiteBase):
    __tablename__ = 'bridge_film_actor'
    
    film_key = Column(Integer,ForeignKey('dim_film.film_key'),primary_key=True)
    actor_key = Column(Integer,ForeignKey('dim_actor.actor_key'),primary_key=True)
    __table_args__ = (
        Index('idx_bridge_fa_film', 'film_key'),
        Index('idx_bridge_fa_actor', 'actor_key'),
    )
class BridgeFilmCategory(LiteBase):
    __tablename__ = 'bridge_film_category'
    
    film_key = Column(Integer, ForeignKey('dim_film.film_key'),primary_key=True)
    category_key = Column(Integer, ForeignKey('dim_category.category_key'),primary_key=True)
    __table_args__ = (
        Index('idx_bridge_fc_film', 'film_key'),
        Index('idx_bridge_fc_category', 'category_key'),
    )
#Facts

class FactRental(LiteBase):
    __tablename__ = 'fact_rental'
    
    fact_rental_key = Column(Integer, primary_key=True, autoincrement=True)
    rental_id = Column(Integer)
    date_key_rented = Column(Integer, ForeignKey('dim_date.date_key'))
    date_key_returned = Column(Integer, ForeignKey('dim_date.date_key'))
    film_key = Column(Integer, ForeignKey('dim_film.film_key'))
    store_key = Column(Integer, ForeignKey('dim_store.store_key'))
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'))
    staff_id = Column(Integer)
    rental_duration_days = Column(Integer)
    last_update = Column(String)
    __table_args__ = (
        Index('idx_fact_rental_cust', 'customer_key'),
        Index('idx_fact_rental_film', 'film_key'),
        Index('idx_fact_rental_store', 'store_key'),
        Index('idx_fact_rental_id', 'rental_id'),
    )

class FactPayment(LiteBase):
    __tablename__ = 'fact_payment'
    
    fact_payment_key = Column(Integer, primary_key=True, autoincrement=True)
    payment_id = Column(Integer)
    date_key_paid = Column(Integer, ForeignKey('dim_date.date_key'))
    rental_id = Column(Integer) #Adding this so I can calculate per-store validation
    customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'))
    store_key = Column(Integer, ForeignKey('dim_store.store_key'))
    staff_id = Column(Integer)
    amount = Column(Float)
    last_update = Column(String)
    __table_args__ = (
        Index('idx_fact_payment_rental', 'rental_id'),
        Index('idx_fact_payment_cust', 'customer_key'),
    )

#SyncState

class SyncState(LiteBase):
    __tablename__ = 'sync_state'
    table_name = Column(String(50), primary_key=True)
    last_sync_timestamp = Column(DateTime, nullable=False)

#MySQL

class SakilaMixin:
    """Standard Sakila tracking column"""
    last_update = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

class Language(SakilaBase, SakilaMixin):
    __tablename__ = 'language'
    language_id = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False)

class Category(SakilaBase, SakilaMixin):
    __tablename__ = 'category'
    category_id = Column(SmallInteger, primary_key=True)
    name = Column(String(25), nullable=False)

class Actor(SakilaBase, SakilaMixin):
    __tablename__ = 'actor'
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)

class Country(SakilaBase, SakilaMixin):
    __tablename__ = 'country'
    country_id = Column(SmallInteger, primary_key=True)
    country = Column(String(50), nullable=False)

class City(SakilaBase, SakilaMixin):
    __tablename__ = 'city'
    city_id = Column(Integer, primary_key=True)
    city = Column(String(50), nullable=False)
    country_id = Column(SmallInteger, ForeignKey('country.country_id'))
    country = relationship("Country")
class Customer(SakilaBase, SakilaMixin):
    __tablename__ = 'customer'

    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, ForeignKey('store.store_id'), nullable=False)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    email = Column(String(50), nullable=True)
    address_id = Column(Integer, ForeignKey('address.address_id'), nullable=False)
    active = Column(Boolean, default=True, nullable=False)
    create_date = Column(DateTime, default=datetime.utcnow, nullable=False)


    store = relationship("Store")
    address = relationship("Address")
class Store(SakilaBase, SakilaMixin):
    __tablename__ = 'store'

    store_id = Column(Integer, primary_key=True, autoincrement=True)
    manager_staff_id = Column(Integer, ForeignKey('staff.staff_id'), nullable=False, unique=True)
    address_id = Column(Integer, ForeignKey('address.address_id'), nullable=False)
    manager = relationship("Staff", foreign_keys=[manager_staff_id])
    address = relationship("Address", foreign_keys=[address_id])
class Staff(SakilaBase, SakilaMixin):
    __tablename__ = 'staff'

    staff_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    address_id = Column(Integer, ForeignKey('address.address_id'), nullable=False)
    
    email = Column(String(50), nullable=True)
    store_id = Column(Integer, ForeignKey('store.store_id'), nullable=False)
    active = Column(Boolean, default=True, nullable=False)
    username = Column(String(16), nullable=False)
    password = Column(String(40), nullable=True)

    store = relationship("Store", foreign_keys=[store_id])
    address = relationship("Address", foreign_keys=[address_id])

class Address(SakilaBase, SakilaMixin):
    __tablename__ = 'address'
    address_id = Column(Integer, primary_key=True)
    address = Column(String(50), nullable=False)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    postal_code = Column(String(10))

    city = relationship("City")

class Film(SakilaBase, SakilaMixin):
    __tablename__ = 'film'
    film_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    release_year = Column(Integer)
    language_id = Column(SmallInteger, ForeignKey('language.language_id'))
    rating = Column(String(10))
    length = Column(Integer)

    category = relationship("Category", secondary="film_category")
    actor = relationship("Actor", secondary="film_actor")
    language = relationship("Language", foreign_keys=[language_id])
class Rental(SakilaBase, SakilaMixin):
    __tablename__ = 'rental'
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime, nullable=False) 
    inventory_id = Column(Integer, ForeignKey('inventory.inventory_id'))
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    return_date = Column(DateTime)
    staff_id = Column(SmallInteger, ForeignKey('staff.staff_id'))
    inventory = relationship("Inventory")
    customer = relationship("Customer", foreign_keys=[customer_id])
class Payment(SakilaBase, SakilaMixin):
    __tablename__ = 'payment'
    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    staff_id = Column(SmallInteger, ForeignKey('staff.staff_id'))
    rental_id = Column(Integer, ForeignKey('rental.rental_id'))
    amount = Column(Numeric(5, 2), nullable=False)
    payment_date = Column(DateTime, nullable=False)

class Inventory(SakilaBase, SakilaMixin):
    __tablename__ = 'inventory'
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'), nullable=False)
    store_id = Column(Integer, ForeignKey('store.store_id'), nullable=False)

class FilmActor(SakilaBase, SakilaMixin):
    __tablename__ = 'film_actor'
    actor_id = Column(Integer, ForeignKey('actor.actor_id'), primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)

class FilmCategory(SakilaBase, SakilaMixin):
    __tablename__ = 'film_category'
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)
    category_id = Column(SmallInteger, ForeignKey('category.category_id'), primary_key=True)

