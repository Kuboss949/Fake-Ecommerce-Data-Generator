from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from FakeDataGenerator import FakeDataGenerator
from SessionInitiator import SessionInitiator
from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment
from datetime import date, datetime
import encodings
import codecs
# rejestracja kodowania cp1250, wyrzucało błąd dla firebirda
try:
    codecs.lookup('cp1250')
except LookupError:
    import encodings.cp1250

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modele import Base
from FakeDataGenerator import FakeDataGenerator



init = SessionInitiator(drop_data=True)

def main():
    print("--- INICJALIZACJA BAZ DANYCH ---")

    # Inicjalizacja wszystkich baz danych
    session_fb = init.initiate_fb()
    session_maria = init.initiate_maria()
    orient_client = init.initiate_orient()
    init.initiate_cassandra()

    # Inicjalizujemy generator ze wszystkimi sesjami
    generator = FakeDataGenerator(
        main_session=session_fb,
        mirror_session=session_maria,
        orient_client=orient_client
    )

    # Uruchamiamy generowanie
    generator.run_generation(
        num_users=10,
        num_customers=50,
        num_orders=100
    )


if __name__ == '__main__':
    main()

