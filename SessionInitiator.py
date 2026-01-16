from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modele import Base
import pyorientdb as pyorient
from cassandra_tables import init_cassandra_schema

class SessionInitiator:
    def __init__(self, drop_data = False):
        self.drop_data = drop_data


    def initiate_fb(self):
        FB_URL = "firebird+firebird://sysdba:admin123@localhost:3050//var/lib/firebird/data/mirror.fdb?charset=UTF8"
        engine_fb = create_engine(FB_URL)
        SessionFb = sessionmaker(bind=engine_fb)
        print("Tworzenie tabel w Firebird...")
        Base.metadata.create_all(engine_fb)
        session_fb = SessionFb()
        return session_fb


    def initiate_maria(self):
        MARIA_URL = "mysql+pymysql://root:my-secret-pw@localhost:3306/company_db"
        engine_maria = create_engine(MARIA_URL)
        SessionMaria = sessionmaker(bind=engine_maria)
        print("Tworzenie tabel w MariaDB...")
        Base.metadata.create_all(engine_maria)
        session_maria = SessionMaria()
        return session_maria


    def initiate_orient(self):
        DB_NAME = 'company'
        DB_USER = 'root'
        DB_PASS = 'root'

        print(f"🥑 [OrientDB] Łączenie z bazą '{DB_NAME}'...")
        try:
            orient_client = pyorient.OrientDB("localhost", 2424)
            orient_client.connect(DB_USER, DB_PASS)

            # Twardy reset bazy
            if orient_client.db_exists(DB_NAME, pyorient.STORAGE_TYPE_PLOCAL):
                print(f"   -> Usuwanie starej bazy '{DB_NAME}'...")
                orient_client.db_drop(DB_NAME)

            print(f"   -> Tworzenie nowej bazy...")
            orient_client.db_create(DB_NAME, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)

            # Otwarcie bazy
            orient_client.db_open(DB_NAME, DB_USER, DB_PASS)

            # TWORZENIE SCHEMATU (KLAS) - Przeniesione tutaj!
            print("   -> Tworzenie klas (schema)...")
            classes = ["CUSTOMER", "CUSTOMER_ORDER", "INVOICE", "ORDER_ITEM", "PAYMENT", "PRODUCT", "SYS_USER"]
            for cls in classes:
                orient_client.command(f"create class {cls} extends V")

            print("   -> Gotowe.")
            return orient_client  # ZWRACAMY KLIENTA!

        except Exception as e:
            print(f"❌ Błąd OrientDB: {e}")
            raise e

    def initiate_cassandra(self):
        print("👁️ [Cassandra] Inicjalizacja keyspace i tabel...")
        init_cassandra_schema()