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

        print("Tworzenie tabel w Firebird...")
        Base.metadata.create_all(engine_fb)
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

        print(f"🥑 [OrientDB] Łączenie z bazą '{DB_NAME}'...")
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
                except:
                    # Klasa już istnieje, pomijamy
                    pass

            # TWORZENIE FUNKCJI fill_edge
            print("   -> Tworzenie funkcji fill_edge...")
            fill_edge_function = '''CREATE FUNCTION fill_edge "LET customers = (SELECT FROM CUSTOMER); FOREACH (c IN $customers) {CREATE EDGE Customer_to_invoice FROM $c TO (SELECT FROM INVOICE WHERE CUSTOMER_ID = $c.CUSTOMER_ID);}
LET invoices = (SELECT FROM INVOICE); FOREACH (i IN $invoices) { CREATE EDGE Invoice_to_payment FROM $i TO (SELECT FROM PAYMENT WHERE INVOICE_ID = $i.INVOICE_ID);}
LET customers = (SELECT FROM CUSTOMER); FOREACH (c IN $customers) {CREATE EDGE Customer_to_order FROM $c TO (SELECT FROM CUSTOMER_ORDER WHERE CUSTOMER_ID = $c.CUSTOMER_ID);}
LET orders = (SELECT FROM CUSTOMER_ORDER); FOREACH (o IN $orders) {CREATE EDGE Order_to_invoice FROM $o TO (SELECT FROM INVOICE WHERE ORDER_ID = $o.ORDER_ID);}
LET users = (SELECT FROM SYS_USER); FOREACH (u IN $users) {CREATE EDGE User_to_invoice FROM $u TO (SELECT FROM INVOICE WHERE CREATED_BY = $u.USER_ID);}
LET orders = (SELECT FROM CUSTOMER_ORDER); FOREACH (o IN $orders) {CREATE EDGE Order_to_order_item FROM $o TO (SELECT FROM ORDER_ITEM WHERE ORDER_ID = $o.ORDER_ID);}
LET products = (SELECT FROM PRODUCT); FOREACH (p IN $products) {CREATE EDGE Product_to_order_item FROM $p TO (SELECT FROM ORDER_ITEM WHERE PRODUCT_ID = $p.PRODUCT_ID);}"
LANGUAGE sql'''
            try:
                orient_client.command(fill_edge_function)
            except:
                # Funkcja już istnieje, pomijamy
                pass

            print("   -> Gotowe.")
            return orient_client  # ZWRACAMY KLIENTA!

        except Exception as e:
            print(f"❌ Błąd OrientDB: {e}")
            raise e

    def initiate_cassandra(self):
        print("👁️ [Cassandra] Inicjalizacja keyspace i tabel...")

        if self.drop_data:
            print("Usuwanie struktury Cassandra...")
            drop_cassandra_schema()

        init_cassandra_schema()