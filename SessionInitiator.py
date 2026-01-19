from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modele import Base
import pyorientdb as pyorient
from cassandra_tables import init_cassandra_schema, drop_cassandra_schema

class SessionInitiator:
    def __init__(self, drop_data = False):
        self.drop_data = drop_data


    def initiate_fb(self):
        FB_URL = "firebird+firebird://sysdba:admin123@localhost:3050//var/lib/firebird/data/mirror.fdb?charset=UTF8"
        engine_fb = create_engine(FB_URL)
        SessionFb = sessionmaker(bind=engine_fb)

        if self.drop_data:
            print("Usuwanie struktury Firebird...")
            Base.metadata.drop_all(engine_fb)

        # print("Tworzenie tabel w Firebird...")
        # Base.metadata.create_all(engine_fb)
        session_fb = SessionFb()
        return session_fb


    def initiate_maria(self):
        MARIA_URL = "mysql+pymysql://root:my-secret-pw@localhost:3306/company_db"
        engine_maria = create_engine(MARIA_URL)
        SessionMaria = sessionmaker(bind=engine_maria)

        if self.drop_data:
            print("Usuwanie struktury MariaDB...")
            Base.metadata.drop_all(engine_maria)

        print("Tworzenie tabel w MariaDB...")
        Base.metadata.create_all(engine_maria)
        session_maria = SessionMaria()
        return session_maria


    def initiate_orient(self):
        DB_NAME = 'company'
        DB_USER = 'root'
        DB_PASS = 'root'

        print(f"[OrientDB] Laczenie z baza '{DB_NAME}'...")
        try:
            orient_client = pyorient.OrientDB("localhost", 2424)
            orient_client.connect(DB_USER, DB_PASS)

            # Drop bazy jeśli self.drop_data jest True
            if self.drop_data and orient_client.db_exists(DB_NAME, pyorient.STORAGE_TYPE_PLOCAL):
                print(f"   -> Usuwanie starej bazy '{DB_NAME}'...")
                orient_client.db_drop(DB_NAME)

            # Tworzenie bazy jeśli nie istnieje
            if not orient_client.db_exists(DB_NAME, pyorient.STORAGE_TYPE_PLOCAL):
                print(f"   -> Tworzenie nowej bazy...")
                orient_client.db_create(DB_NAME, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)

            # Otwarcie bazy
            orient_client.db_open(DB_NAME, DB_USER, DB_PASS)

            # TWORZENIE SCHEMATU (KLAS WIERZCHOŁKÓW)
            print("   -> Tworzenie klas wierzchołków (V)...")
            vertex_classes = ["CUSTOMER", "CUSTOMER_ORDER", "INVOICE", "ORDER_ITEM", "PAYMENT", "PRODUCT", "SYS_USER"]
            for cls in vertex_classes:
                try:
                    orient_client.command(f"create class {cls} extends V")
                except:
                    # Klasa już istnieje, pomijamy
                    pass

            # TWORZENIE KLAS KRAWĘDZI
            print("   -> Tworzenie klas krawędzi (E)...")
            edge_classes = [
                "Customer_to_invoice",
                "Invoice_to_payment",
                "Customer_to_order",
                "Order_to_invoice",
                "User_to_invoice",
                "Order_to_order_item",
                "Product_to_order_item"
            ]
            for cls in edge_classes:
                try:
                    orient_client.command(f"CREATE CLASS {cls} EXTENDS E")
                except Exception as e:
                    # Klasa już istnieje, pomijamy
                    pass

            # TWORZENIE INDEKSÓW - kluczowe dla wydajności przy dużych zbiorach danych
            print("   -> Tworzenie indeksów...")
            indexes = [
                ("CUSTOMER", "CUSTOMER_ID", "UNIQUE"),
                ("SYS_USER", "USER_ID", "UNIQUE"),
                ("PRODUCT", "PRODUCT_ID", "UNIQUE"),
                ("CUSTOMER_ORDER", "ORDER_ID", "UNIQUE"),
                ("CUSTOMER_ORDER", "CUSTOMER_ID", "NOTUNIQUE"),
                ("ORDER_ITEM", "ORDER_ITEM_ID", "UNIQUE"),
                ("ORDER_ITEM", "ORDER_ID", "NOTUNIQUE"),
                ("ORDER_ITEM", "PRODUCT_ID", "NOTUNIQUE"),
                ("INVOICE", "INVOICE_ID", "UNIQUE"),
                ("INVOICE", "CUSTOMER_ID", "NOTUNIQUE"),
                ("INVOICE", "ORDER_ID", "NOTUNIQUE"),
                ("INVOICE", "CREATED_BY", "NOTUNIQUE"),
                ("PAYMENT", "PAYMENT_ID", "UNIQUE"),
                ("PAYMENT", "INVOICE_ID", "NOTUNIQUE"),
            ]
            for cls, prop, idx_type in indexes:
                try:
                    # Najpierw tworzymy właściwość (jeśli nie istnieje)
                    orient_client.command(f"CREATE PROPERTY {cls}.{prop} IF NOT EXISTS INTEGER")
                    # Następnie tworzymy indeks
                    orient_client.command(f"CREATE INDEX {cls}.{prop} {idx_type}")
                except Exception as e:
                    # Indeks już istnieje lub inny błąd, pomijamy
                    pass

            print("   -> Gotowe.")
            return orient_client  # ZWRACAMY KLIENTA!

        except Exception as e:
            print(f"[ERROR] Blad OrientDB: {e}")
            raise e

    def initiate_cassandra(self):
        print("[Cassandra] Inicjalizacja keyspace i tabel...")

        if self.drop_data:
            print("Usuwanie struktury Cassandra...")
            drop_cassandra_schema()

        init_cassandra_schema()