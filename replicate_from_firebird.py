"""
Skrypt do replikacji danych z Firebirda do MariaDB, OrientDB i Cassandry.
Uruchamiany osobno, gdy generowanie danych zostalo zakonczone, ale replikacja sie nie powiodla.

UWAGA: Ustawienie DROP_BEFORE_REPLICATE = True usunie WSZYSTKIE dane z docelowych baz!
"""
import codecs
import sys
from collections import defaultdict
from sqlalchemy import create_engine, select, inspect, text
from sqlalchemy.orm import sessionmaker
import pyorientdb as pyorient

from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment
from cassandra_tables import (
    UsersByRole, CustomerCountByCity, PaymentsByAmountRange,
    ProductStockAggregates, AllInvoicesWithDetails,
    SalesStatsByCountryAll, CustomerLeaderboardAll,
    InvoicesByCustomerName, OrderItemsByProduct,
    init_cassandra_schema
)
from cassandra.cqlengine.query import BatchQuery
from cassandra.cluster import Cluster
try:
    codecs.lookup('cp1250')
except LookupError:
    import encodings.cp1250

# ============== KONFIGURACJA ==============
FIREBIRD_URL = "firebird+firebird://sysdba:admin123@localhost:3050//var/lib/firebird/data/mirror.fdb?charset=UTF8"
MARIADB_URL = "mysql+pymysql://root:my-secret-pw@localhost:3306/company_db"
ORIENTDB_HOST = "localhost"
ORIENTDB_PORT = 2424
ORIENTDB_USER = "root"
ORIENTDB_PASS = "root"
ORIENTDB_DB = "company"
CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'my_keyspace'

# Co replikowac
REPLICATE_TO_MARIADB = True
REPLICATE_TO_ORIENTDB = True
REPLICATE_TO_CASSANDRA = True

# UWAGA: Ustawienie True usunie WSZYSTKIE dane przed replikacja!
DROP_BEFORE_REPLICATE = True


# ============== FUNKCJE DROPOWANIA DANYCH ==============

def drop_mariadb_data(session):
    """Usuwa wszystkie dane z tabel MariaDB (w odpowiedniej kolejnosci - FK)."""
    print("[MariaDB] Usuwanie danych...")

    # Wylacz sprawdzanie kluczy obcych
    session.execute(text("SET FOREIGN_KEY_CHECKS = 0"))

    tables = ['PAYMENT', 'INVOICE', 'ORDER_ITEM', 'CUSTOMER_ORDER', 'CUSTOMER', 'PRODUCT', 'SYS_USER']
    for table in tables:
        try:
            session.execute(text(f"TRUNCATE TABLE {table}"))
            print(f"   -> TRUNCATE {table} OK")
        except Exception as e:
            print(f"   -> TRUNCATE {table} ERROR: {e}")

    # Wlacz sprawdzanie kluczy obcych
    session.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
    session.commit()
    print("[MariaDB] Dane usuniete!")


def drop_orientdb_data(client):
    """Usuwa wszystkie wierzcholki i krawedzie z OrientDB."""
    print("[OrientDB] Usuwanie danych...")

    # Najpierw usuwamy krawedzie
    edge_classes = [
        "Customer_to_invoice", "Invoice_to_payment", "Customer_to_order",
        "Order_to_invoice", "User_to_invoice", "Order_to_order_item", "Product_to_order_item"
    ]
    for edge_class in edge_classes:
        try:
            client.command(f"DELETE EDGE {edge_class}")
            print(f"   -> DELETE EDGE {edge_class} OK")
        except Exception as e:
            print(f"   -> DELETE EDGE {edge_class} SKIP: {e}")

    # Potem usuwamy wierzcholki
    vertex_classes = ["PAYMENT", "INVOICE", "ORDER_ITEM", "CUSTOMER_ORDER", "CUSTOMER", "PRODUCT", "SYS_USER"]
    for vertex_class in vertex_classes:
        try:
            client.command(f"DELETE VERTEX {vertex_class}")
            print(f"   -> DELETE VERTEX {vertex_class} OK")
        except Exception as e:
            print(f"   -> DELETE VERTEX {vertex_class} SKIP: {e}")

    print("[OrientDB] Dane usuniete!")


def drop_cassandra_data():
    """Usuwa wszystkie dane z tabel Cassandra (TRUNCATE lub DROP/CREATE dla counterow)."""
    print("[Cassandra] Usuwanie danych...")

    cluster = Cluster(CASSANDRA_HOSTS)
    # Najpierw polacz bez keyspace zeby sprawdzic czy istnieje
    session = cluster.connect()

    # Sprawdz czy keyspace istnieje, jesli nie - utworz
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(CASSANDRA_KEYSPACE)
    except Exception as e:
        print(f"   -> Keyspace: {e}")
        session.shutdown()
        cluster.shutdown()
        return

    # Zwykle tabele - TRUNCATE
    regular_tables = [
        'users_by_role', 'payments_by_amount_range',
        'all_invoices_with_details', 'sales_stats_all',
        'customer_leaderboard_all', 'invoices_by_customer_name', 'order_items_by_product'
    ]

    for table in regular_tables:
        try:
            session.execute(f"TRUNCATE {table}")
            print(f"   -> TRUNCATE {table} OK")
        except Exception as e:
            print(f"   -> TRUNCATE {table} SKIP: {e}")

    # Tabele z counterami - DROP i CREATE (TRUNCATE nie dziala dla counterow)
    counter_tables = [
        ('customer_count_by_city', """
            CREATE TABLE IF NOT EXISTS customer_count_by_city (
                bucket text,
                city text,
                customer_count counter,
                PRIMARY KEY (bucket, city)
            )
        """),
        ('product_stock_aggregates', """
            CREATE TABLE IF NOT EXISTS product_stock_aggregates (
                price_bucket text,
                dummy int,
                total_stock counter,
                PRIMARY KEY (price_bucket, dummy)
            )
        """)
    ]

    for table_name, create_stmt in counter_tables:
        try:
            session.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"   -> DROP TABLE {table_name} OK")
            session.execute(create_stmt)
            print(f"   -> CREATE TABLE {table_name} OK")
        except Exception as e:
            print(f"   -> {table_name} SKIP: {e}")

    session.shutdown()
    cluster.shutdown()
    print("[Cassandra] Dane usuniete!")


def create_firebird_session():
    """Tworzy sesje do Firebirda (tylko odczyt)."""
    print("[Firebird] Laczenie...")
    engine = create_engine(FIREBIRD_URL, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("[Firebird] Polaczono!")
    return session, engine


def create_mariadb_session():
    """Tworzy sesje do MariaDB z odpowiednimi ustawieniami dla dlugich operacji."""
    print("[MariaDB] Laczenie...")
    engine = create_engine(
        MARIADB_URL,
        echo=False,
        pool_pre_ping=True,  # Sprawdza polaczenie przed uzyciem
        pool_recycle=300,    # Recykluj polaczenia co 5 min
        connect_args={
            'connect_timeout': 60,
            'read_timeout': 300,
            'write_timeout': 300,
        }
    )
    # Tworzymy tabele jesli nie istnieja
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("[MariaDB] Polaczono!")
    return session, engine


def replicate_to_mariadb(source_session, target_session, batch_size=500):
    """Replikuje dane z Firebirda do MariaDB w malych partiach."""
    print("\n========== REPLIKACJA DO MARIADB ==========")

    models_order = [
        SysUser,
        Product,
        Customer,
        CustomerOrder,
        OrderItem,
        Invoice,
        Payment
    ]

    for model in models_order:
        model_name = model.__tablename__
        column_attrs = list(inspect(model).mapper.column_attrs)

        # Sprawdz ile juz jest w MariaDB (tylko jesli nie dropujemy)
        if not DROP_BEFORE_REPLICATE:
            existing_count = target_session.query(model).count()
            if existing_count > 0:
                print(f"   [{model_name}] Juz istnieje {existing_count} rekordow - pomijam")
                continue

        # Policz rekordy w Firebird
        total_count = source_session.query(model).count()
        print(f"   [{model_name}] Kopiowanie {total_count} rekordow...")

        if total_count == 0:
            continue

        # Stream i zapisuj w partiach
        target_objects_buffer = []
        processed = 0

        for obj in source_session.query(model).yield_per(batch_size):
            data = {c.key: getattr(obj, c.key) for c in column_attrs}
            target_objects_buffer.append(model(**data))
            processed += 1

            if len(target_objects_buffer) >= batch_size:
                try:
                    target_session.bulk_save_objects(target_objects_buffer)
                    target_session.commit()
                    print(f"      ... {processed}/{total_count}")
                except Exception as e:
                    print(f"      [ERROR] {e}")
                    target_session.rollback()
                    # Proba ponownego polaczenia
                    try:
                        target_session.close()
                        target_session = create_mariadb_session()[0]
                        target_session.bulk_save_objects(target_objects_buffer)
                        target_session.commit()
                        print(f"      ... {processed}/{total_count} (po reconnect)")
                    except Exception as e2:
                        print(f"      [FATAL] Nie udalo sie zapisac: {e2}")
                        raise e2
                target_objects_buffer = []

        # Zapisz reszte
        if target_objects_buffer:
            try:
                target_session.bulk_save_objects(target_objects_buffer)
                target_session.commit()
                print(f"      ... {processed}/{total_count} (DONE)")
            except Exception as e:
                print(f"      [ERROR] {e}")
                target_session.rollback()

        # Wyczysc cache
        source_session.expire_all()

    print("[MariaDB] Replikacja zakonczona!")
    return target_session


def create_orient_connection():
    """Tworzy polaczenie do OrientDB."""
    client = pyorient.OrientDB(ORIENTDB_HOST, ORIENTDB_PORT)
    client.connect(ORIENTDB_USER, ORIENTDB_PASS)
    client.db_open(ORIENTDB_DB, ORIENTDB_USER, ORIENTDB_PASS)
    return client


def escape_orient_string(value):
    """Escapuje znaki specjalne dla OrientDB."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')


def batch_insert_orient(client, table_name, records, batch_size=500):
    """Wstawia rekordy do OrientDB w partiach."""
    id_to_rid = {}
    total = len(records)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        # Budujemy batch script
        batch_script = "begin\n"
        for idx, record in enumerate(batch):
            fields = []
            for key, value in record.items():
                if isinstance(value, str):
                    fields.append(f"{key} = '{escape_orient_string(value)}'")
                elif value is None:
                    fields.append(f"{key} = null")
                else:
                    fields.append(f"{key} = {value}")
            fields_str = ", ".join(fields)
            batch_script += f"let $r{idx} = INSERT INTO {table_name} SET {fields_str}\n"

        batch_script += "commit\n"
        batch_script += "return ["
        batch_script += ", ".join([f"$r{idx}" for idx in range(len(batch))])
        batch_script += "]"

        try:
            results = client.batch(batch_script)
            # Mapujemy ID na RID
            for idx, record in enumerate(batch):
                for k in record.keys():
                    if k.endswith("_ID"):
                        if idx < len(results):
                            try:
                                rid = results[idx]._rid if hasattr(results[idx], '_rid') else str(results[idx])
                                id_to_rid[record[k]] = rid
                            except:
                                pass
                        break
        except Exception as e:
            print(f"      [WARN] Batch failed, falling back to single inserts: {e}")
            for record in batch:
                try:
                    fields = []
                    for key, value in record.items():
                        if isinstance(value, str):
                            fields.append(f"{key} = '{escape_orient_string(value)}'")
                        elif value is None:
                            fields.append(f"{key} = null")
                        else:
                            fields.append(f"{key} = {value}")
                    fields_str = ", ".join(fields)
                    result = client.command(f"INSERT INTO {table_name} SET {fields_str}")
                    if result and len(result) > 0:
                        rid = result[0]._rid if hasattr(result[0], '_rid') else str(result[0])
                        for k in record.keys():
                            if k.endswith("_ID"):
                                id_to_rid[record[k]] = rid
                                break
                except:
                    pass

        inserted += len(batch)
        if inserted % 5000 == 0 or inserted == total:
            print(f"      ... {inserted}/{total}")

        # Odswiez polaczenie co 10000 rekordow
        if inserted % 10000 == 0 and inserted < total:
            print("      -> Odswiezanie polaczenia...")
            try:
                client.db_close()
                client.close()
            except:
                pass
            client = create_orient_connection()

    return id_to_rid, client


def create_edges_batch(client, edge_class, from_rid_map, to_rids_by_fk, batch_size=200):
    """Tworzy krawedzie w partiach."""
    print(f"      -> {edge_class}...")
    edges_created = 0
    edge_commands = []

    for fk_id, to_rids in to_rids_by_fk.items():
        if fk_id in from_rid_map and to_rids:
            from_rid = from_rid_map[fk_id]
            for to_rid in to_rids:
                edge_commands.append((from_rid, to_rid))

    total_edges = len(edge_commands)
    if total_edges == 0:
        print(f"         Brak krawedzi")
        return client

    for i in range(0, total_edges, batch_size):
        batch = edge_commands[i:i + batch_size]

        batch_script = "begin\n"
        for idx, (from_rid, to_rid) in enumerate(batch):
            batch_script += f"CREATE EDGE {edge_class} FROM {from_rid} TO {to_rid}\n"
        batch_script += "commit\nreturn true"

        try:
            client.batch(batch_script)
            edges_created += len(batch)
        except Exception as e:
            for from_rid, to_rid in batch:
                try:
                    client.command(f"CREATE EDGE {edge_class} FROM {from_rid} TO {to_rid}")
                    edges_created += 1
                except:
                    pass

        if edges_created % 5000 == 0 and edges_created > 0:
            print(f"         ... {edges_created}/{total_edges}")
            try:
                client.db_close()
                client.close()
            except:
                pass
            client = create_orient_connection()

    print(f"         Utworzono {edges_created} krawedzi")
    return client


def replicate_to_orientdb(source_session):
    """Replikuje dane z Firebirda do OrientDB."""
    print("\n========== REPLIKACJA DO ORIENTDB ==========")

    print("[OrientDB] Laczenie...")
    client = create_orient_connection()
    print("[OrientDB] Polaczono!")

    # Sprawdz czy dane juz istnieja (tylko jesli nie dropujemy)
    if not DROP_BEFORE_REPLICATE:
        try:
            existing = client.command("SELECT count(*) FROM CUSTOMER")
            if existing and len(existing) > 0:
                count = existing[0].oRecordData.get('count', 0)
                if count > 0:
                    print(f"[OrientDB] Juz istnieje {count} klientow - pomijam replikacje")
                    client.close()
                    return
        except:
            pass

    # Cache dla RID
    rid_cache = {
        'customer': {},
        'user': {},
        'product': {},
        'order': {},
        'order_item': {},
        'invoice': {},
        'payment': {},
    }

    # Mapowania dla krawedzi
    fk_mappings = {
        'invoice_by_customer': defaultdict(list),
        'invoice_by_order': defaultdict(list),
        'invoice_by_user': defaultdict(list),
        'order_by_customer': defaultdict(list),
        'order_item_by_order': defaultdict(list),
        'order_item_by_product': defaultdict(list),
        'payment_by_invoice': defaultdict(list),
    }

    # 1. CUSTOMERS
    print("   -> Dodawanie Customers...")
    customers = source_session.execute(select(Customer)).scalars().all()
    customer_records = [
        {
            'CUSTOMER_ID': c.CUSTOMER_ID,
            'NAME': c.NAME,
            'EMAIL': c.EMAIL,
            'PHONE': c.PHONE,
            'ADDRESS': c.ADDRESS,
            'CITY': c.CITY,
            'COUNTRY': c.COUNTRY
        } for c in customers
    ]
    rid_cache['customer'], client = batch_insert_orient(client, 'CUSTOMER', customer_records)
    print(f"      Dodano {len(customers)} klientow")

    # 2. USERS
    print("   -> Dodawanie Users...")
    client = create_orient_connection()
    users = source_session.execute(select(SysUser)).scalars().all()
    user_records = [
        {
            'USER_ID': u.USER_ID,
            'USERNAME': u.USERNAME,
            'PASSWORD_HASH': u.PASSWORD_HASH,
            'NAME': u.NAME,
            'SURNAME': u.SURNAME,
            'EMAIL': u.EMAIL,
            'ROLE': u.ROLE,
            'ACTIVE': str(u.ACTIVE) if u.ACTIVE else '1'
        } for u in users
    ]
    rid_cache['user'], client = batch_insert_orient(client, 'SYS_USER', user_records)
    print(f"      Dodano {len(users)} uzytkownikow")

    # 3. PRODUCTS
    print("   -> Dodawanie Products...")
    client = create_orient_connection()
    products = source_session.execute(select(Product)).scalars().all()
    product_records = [
        {
            'PRODUCT_ID': p.PRODUCT_ID,
            'NAME': p.NAME,
            'DESCRIPTION': p.DESCRIPTION,
            'PRICE': round(float(p.PRICE), 2),
            'STOCK_QUANTITY': p.STOCK_QUANTITY
        } for p in products
    ]
    rid_cache['product'], client = batch_insert_orient(client, 'PRODUCT', product_records)
    print(f"      Dodano {len(products)} produktow")

    # 4. ORDERS
    print("   -> Dodawanie Orders...")
    client = create_orient_connection()
    orders = source_session.execute(select(CustomerOrder)).scalars().all()
    order_records = [
        {
            'ORDER_ID': o.ORDER_ID,
            'CUSTOMER_ID': o.CUSTOMER_ID,
            'ORDER_DATE': str(o.ORDER_DATE),
            'STATUS': o.STATUS,
            'TOTAL_AMOUNT': round(float(o.TOTAL_AMOUNT), 2)
        } for o in orders
    ]
    rid_cache['order'], client = batch_insert_orient(client, 'CUSTOMER_ORDER', order_records)
    for o in orders:
        if o.ORDER_ID in rid_cache['order']:
            fk_mappings['order_by_customer'][o.CUSTOMER_ID].append(rid_cache['order'][o.ORDER_ID])
    print(f"      Dodano {len(orders)} zamowien")

    # 5. ORDER ITEMS
    print("   -> Dodawanie Order Items...")
    client = create_orient_connection()
    order_items = source_session.execute(select(OrderItem)).scalars().all()
    order_item_records = [
        {
            'ORDER_ITEM_ID': oi.ORDER_ITEM_ID,
            'ORDER_ID': oi.ORDER_ID,
            'PRODUCT_ID': oi.PRODUCT_ID,
            'QUANTITY': oi.QUANTITY,
            'UNIT_PRICE': round(float(oi.UNIT_PRICE), 2)
        } for oi in order_items
    ]
    rid_cache['order_item'], client = batch_insert_orient(client, 'ORDER_ITEM', order_item_records)
    for oi in order_items:
        if oi.ORDER_ITEM_ID in rid_cache['order_item']:
            rid = rid_cache['order_item'][oi.ORDER_ITEM_ID]
            fk_mappings['order_item_by_order'][oi.ORDER_ID].append(rid)
            fk_mappings['order_item_by_product'][oi.PRODUCT_ID].append(rid)
    print(f"      Dodano {len(order_items)} pozycji zamowien")

    # 6. INVOICES
    print("   -> Dodawanie Invoices...")
    client = create_orient_connection()
    invoices = source_session.execute(select(Invoice)).scalars().all()
    invoice_records = [
        {
            'INVOICE_ID': i.INVOICE_ID,
            'INVOICE_NUMBER': i.INVOICE_NUMBER,
            'CUSTOMER_ID': i.CUSTOMER_ID,
            'ORDER_ID': i.ORDER_ID,
            'STATUS': i.STATUS,
            'ISSUE_DATE': str(i.ISSUE_DATE),
            'DUE_DATE': str(i.DUE_DATE),
            'TOTAL_AMOUNT': round(float(i.TOTAL_AMOUNT), 2),
            'CREATED_BY': i.CREATED_BY
        } for i in invoices
    ]
    rid_cache['invoice'], client = batch_insert_orient(client, 'INVOICE', invoice_records)
    for i in invoices:
        if i.INVOICE_ID in rid_cache['invoice']:
            rid = rid_cache['invoice'][i.INVOICE_ID]
            fk_mappings['invoice_by_customer'][i.CUSTOMER_ID].append(rid)
            fk_mappings['invoice_by_order'][i.ORDER_ID].append(rid)
            fk_mappings['invoice_by_user'][i.CREATED_BY].append(rid)
    print(f"      Dodano {len(invoices)} faktur")

    # 7. PAYMENTS
    print("   -> Dodawanie Payments...")
    client = create_orient_connection()
    payments = source_session.execute(select(Payment)).scalars().all()
    payment_records = [
        {
            'PAYMENT_ID': p.PAYMENT_ID,
            'INVOICE_ID': p.INVOICE_ID,
            'PAYMENT_DATE': str(p.PAYMENT_DATE),
            'AMOUNT': round(float(p.AMOUNT), 2),
            'METHOD': p.METHOD,
            'CONFIRMED': p.CONFIRMED
        } for p in payments
    ]
    rid_cache['payment'], client = batch_insert_orient(client, 'PAYMENT', payment_records)
    for p in payments:
        if p.PAYMENT_ID in rid_cache['payment']:
            fk_mappings['payment_by_invoice'][p.INVOICE_ID].append(rid_cache['payment'][p.PAYMENT_ID])
    print(f"      Dodano {len(payments)} platnosci")

    # KRAWEDZIE
    print("   -> Tworzenie krawedzi...")
    client = create_orient_connection()

    client = create_edges_batch(client, 'Customer_to_invoice', rid_cache['customer'], fk_mappings['invoice_by_customer'])
    client = create_edges_batch(client, 'Invoice_to_payment', rid_cache['invoice'], fk_mappings['payment_by_invoice'])
    client = create_edges_batch(client, 'Customer_to_order', rid_cache['customer'], fk_mappings['order_by_customer'])
    client = create_edges_batch(client, 'Order_to_invoice', rid_cache['order'], fk_mappings['invoice_by_order'])
    client = create_edges_batch(client, 'User_to_invoice', rid_cache['user'], fk_mappings['invoice_by_user'])
    client = create_edges_batch(client, 'Order_to_order_item', rid_cache['order'], fk_mappings['order_item_by_order'])
    client = create_edges_batch(client, 'Product_to_order_item', rid_cache['product'], fk_mappings['order_item_by_product'])

    print("[OrientDB] Replikacja zakonczona!")
    try:
        client.close()
    except:
        pass


def replicate_to_cassandra(source_session):
    """Replikuje dane z Firebirda do Cassandry - NOWA STRUKTURA TABEL."""
    print("\n========== REPLIKACJA DO CASSANDRA ==========")

    from cassandra.cqlengine import connection
    from cassandra.cqlengine.management import sync_table, create_keyspace_simple

    # Polacz z Cassandra - najpierw bez keyspace
    print("[Cassandra] Laczenie...")
    connection.setup(CASSANDRA_HOSTS, 'system', protocol_version=4)

    # Utworz keyspace jesli nie istnieje
    print("[Cassandra] Tworzenie keyspace...")
    create_keyspace_simple(CASSANDRA_KEYSPACE, replication_factor=1)

    # Teraz polacz z keyspace
    connection.setup(CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, protocol_version=4)

    # Synchronizuj tabele (utworz jesli nie istnieja)
    print("[Cassandra] Synchronizacja tabel...")
    sync_table(UsersByRole)
    sync_table(CustomerCountByCity)
    sync_table(PaymentsByAmountRange)
    sync_table(ProductStockAggregates)
    sync_table(AllInvoicesWithDetails)
    sync_table(SalesStatsByCountryAll)
    sync_table(CustomerLeaderboardAll)
    sync_table(InvoicesByCustomerName)
    sync_table(OrderItemsByProduct)

    # Potrzebujemy tez zwyklej sesji dla counterow
    cluster = Cluster(CASSANDRA_HOSTS)
    cql_session = cluster.connect(CASSANDRA_KEYSPACE)
    print("[Cassandra] Polaczono!")

    # Pobierz dane zrodlowe
    users = source_session.execute(select(SysUser)).scalars().all()
    customers = source_session.execute(select(Customer)).scalars().all()
    products = source_session.execute(select(Product)).scalars().all()
    payments = source_session.execute(select(Payment)).scalars().all()
    invoices = source_session.execute(select(Invoice)).scalars().all()
    orders = source_session.execute(select(CustomerOrder)).scalars().all()
    order_items = source_session.execute(select(OrderItem)).scalars().all()

    # Budujemy mapy dla szybkiego dostepu
    customer_map = {c.CUSTOMER_ID: c for c in customers}
    payment_map = {p.INVOICE_ID: p for p in payments}
    product_map = {p.PRODUCT_ID: p for p in products}
    user_map = {u.USER_ID: u.USERNAME for u in users}
    order_to_customer = {o.ORDER_ID: customer_map.get(o.CUSTOMER_ID) for o in orders}
    order_to_invoice = {inv.ORDER_ID: inv.INVOICE_ID for inv in invoices}

    # 1. USERS BY ROLE (bez zmian)
    print("   -> Dodawanie Users by Role...")
    batch = []
    for u in users:
        batch.append({
            'role': u.ROLE,
            'user_id': u.USER_ID,
            'username': u.USERNAME,
            'name': f"{u.NAME} {u.SURNAME}",
            'email': u.EMAIL
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    UsersByRole.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                UsersByRole.batch(b).create(**item)
    print(f"      Dodano {len(users)} uzytkownikow")

    # 2. CUSTOMER COUNT BY CITY (COUNTER) - NOWA TABELA
    print("   -> Aktualizowanie Customer Count by City (counters)...")
    city_counts = defaultdict(int)
    for c in customers:
        city_counts[c.CITY] += 1

    for city, count in city_counts.items():
        cql_session.execute(
            "UPDATE customer_count_by_city SET customer_count = customer_count + %s WHERE bucket = 'all' AND city = %s",
            (count, city)
        )
    print(f"      Zaktualizowano {len(city_counts)} miast")

    # 3. PAYMENTS BY AMOUNT RANGE - NOWA TABELA
    print("   -> Dodawanie Payments by Amount Range...")
    batch = []
    for p in payments:
        amount = float(p.AMOUNT)
        # Klasyfikuj do bucketa
        if amount < 10000:
            bucket = 'under_10k'
        elif amount <= 40000:
            bucket = '10k-40k'
        else:
            bucket = 'over_40k'

        batch.append({
            'amount_bucket': bucket,
            'amount': amount,
            'payment_id': p.PAYMENT_ID,
            'invoice_id': p.INVOICE_ID,
            'method': p.METHOD,
            'payment_date': p.PAYMENT_DATE,
            'confirmed': bool(p.CONFIRMED)
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    PaymentsByAmountRange.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                PaymentsByAmountRange.batch(b).create(**item)
    print(f"      Dodano {len(payments)} platnosci")

    # 4. PRODUCT STOCK AGGREGATES (COUNTER) - NOWA TABELA
    print("   -> Aktualizowanie Product Stock Aggregates (counters)...")
    stock_by_bucket = defaultdict(int)
    for p in products:
        price = float(p.PRICE)
        if price < 100:
            bucket = 'under_100'
        elif price <= 500:
            bucket = '100_500'
        else:
            bucket = 'over_500'
        stock_by_bucket[bucket] += p.STOCK_QUANTITY

    for bucket, total_stock in stock_by_bucket.items():
        cql_session.execute(
            "UPDATE product_stock_aggregates SET total_stock = total_stock + %s WHERE price_bucket = %s AND dummy = 1",
            (total_stock, bucket)
        )
    print(f"      Zaktualizowano {len(stock_by_bucket)} bucketow cenowych")

    # 5. ALL INVOICES WITH DETAILS - NOWA TABELA (partycjonowana po roku)
    print("   -> Dodawanie All Invoices with Details...")
    batch = []
    for i in invoices:
        customer = customer_map.get(i.CUSTOMER_ID)
        payment = payment_map.get(i.INVOICE_ID)
        year = i.ISSUE_DATE.year if hasattr(i.ISSUE_DATE, 'year') else i.ISSUE_DATE.date().year

        batch.append({
            'year': year,
            'invoice_id': i.INVOICE_ID,
            'invoice_number': i.INVOICE_NUMBER,
            'total_amount': float(i.TOTAL_AMOUNT),
            'status': i.STATUS,
            'issue_date': i.ISSUE_DATE.date() if hasattr(i.ISSUE_DATE, 'date') else i.ISSUE_DATE,
            'due_date': i.DUE_DATE.date() if hasattr(i.DUE_DATE, 'date') else i.DUE_DATE,
            'past_due': False,
            'customer_id': i.CUSTOMER_ID,
            'customer_name': customer.NAME if customer else 'Unknown',
            'customer_email': customer.EMAIL if customer else '',
            'payment_method': payment.METHOD if payment else 'N/A',
            'payment_amount': float(payment.AMOUNT) if payment else 0.0,
            'payment_confirmed': bool(payment.CONFIRMED) if payment else False
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    AllInvoicesWithDetails.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                AllInvoicesWithDetails.batch(b).create(**item)
    print(f"      Dodano {len(invoices)} faktur")

    # 6. INVOICES BY CUSTOMER NAME (pomocnicza)
    print("   -> Dodawanie Invoices by Customer Name...")
    batch = []
    for i in invoices:
        customer = customer_map.get(i.CUSTOMER_ID)
        batch.append({
            'customer_name': customer.NAME if customer else 'Unknown',
            'invoice_id': i.INVOICE_ID,
            'total_amount': float(i.TOTAL_AMOUNT),
            'status': i.STATUS
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    InvoicesByCustomerName.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                InvoicesByCustomerName.batch(b).create(**item)
    print(f"      Dodano {len(invoices)} wpisow")

    # 7. ORDER ITEMS BY PRODUCT (pomocnicza)
    print("   -> Dodawanie Order Items by Product...")
    batch = []
    for oi in order_items:
        invoice_id = order_to_invoice.get(oi.ORDER_ID, 0)
        batch.append({
            'product_id': oi.PRODUCT_ID,
            'invoice_id': invoice_id,
            'quantity': oi.QUANTITY,
            'unit_price': float(oi.UNIT_PRICE)
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    OrderItemsByProduct.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                OrderItemsByProduct.batch(b).create(**item)
    print(f"      Dodano {len(order_items)} pozycji")

    # 8. SALES STATS ALL - NOWA TABELA (bucket='all')
    print("   -> Agregowanie Sales Stats All...")
    stats_cache = defaultdict(int)
    for oi in order_items:
        customer = order_to_customer.get(oi.ORDER_ID)
        if customer:
            country = customer.COUNTRY
            prod_name = product_map.get(oi.PRODUCT_ID).NAME if product_map.get(oi.PRODUCT_ID) else 'Unknown'
            stats_cache[(country, prod_name)] += oi.QUANTITY

    batch = []
    for (country, prod_name), qty in stats_cache.items():
        batch.append({
            'bucket': 'all',
            'country': country,
            'product_name': prod_name,
            'total_quantity_sum': qty,
            'product_id': 0
        })
        if len(batch) >= 50:
            with BatchQuery() as b:
                for item in batch:
                    SalesStatsByCountryAll.batch(b).create(**item)
            batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                SalesStatsByCountryAll.batch(b).create(**item)
    print(f"      Dodano {len(stats_cache)} statystyk")

    # 9. CUSTOMER LEADERBOARD ALL - NOWA TABELA (bucket='all')
    # Logika zgodna z SQL: ORDER.STATUS='COMPLETED', PAYMENT.CONFIRMED=1, HAVING SUM(qty)>10
    print("   -> Agregowanie Customer Leaderboard All...")

    # Budujemy mapowania
    order_map = {o.ORDER_ID: o for o in orders}
    invoice_to_order = {inv.INVOICE_ID: inv.ORDER_ID for inv in invoices}

    # Znajdz zamowienia COMPLETED z potwierdzonymi platosciami
    valid_orders = set()
    order_invoice_map = {}  # ORDER_ID -> INVOICE
    for inv in invoices:
        order = order_map.get(inv.ORDER_ID)
        if order and order.STATUS == 'COMPLETED':
            payment = payment_map.get(inv.INVOICE_ID)
            if payment and payment.CONFIRMED:
                valid_orders.add(inv.ORDER_ID)
                order_invoice_map[inv.ORDER_ID] = inv

    # Agreguj dane zgodnie z logika SQL
    leaderboard = defaultdict(lambda: {
        'gross_value': 0.0,
        'orders_count': 0,
        'items_count': 0,
        'unique_products': set(),
        'unique_orders': set(),
        'last_invoice': None,
        'agent': 'Unknown',
        'country': ''
    })

    for oi in order_items:
        if oi.ORDER_ID in valid_orders:
            order = order_map.get(oi.ORDER_ID)
            customer = customer_map.get(order.CUSTOMER_ID) if order else None
            inv = order_invoice_map.get(oi.ORDER_ID)

            if customer and inv:
                key = (customer.NAME, customer.COUNTRY, user_map.get(inv.CREATED_BY, 'Unknown'))
                leaderboard[key]['items_count'] += oi.QUANTITY
                leaderboard[key]['gross_value'] += float(oi.QUANTITY * oi.UNIT_PRICE)
                leaderboard[key]['unique_products'].add(oi.PRODUCT_ID)
                leaderboard[key]['unique_orders'].add(oi.ORDER_ID)
                leaderboard[key]['country'] = customer.COUNTRY
                leaderboard[key]['agent'] = user_map.get(inv.CREATED_BY, 'Unknown')
                if leaderboard[key]['last_invoice'] is None or inv.ISSUE_DATE > leaderboard[key]['last_invoice']:
                    leaderboard[key]['last_invoice'] = inv.ISSUE_DATE

    batch = []
    for (customer_name, country, agent), data in leaderboard.items():
        # HAVING SUM(oi.QUANTITY) > 10
        if data['items_count'] > 10:
            batch.append({
                'bucket': 'all',
                'gross_value_brutto': data['gross_value'],
                'customer_name': customer_name,
                'country': country,
                'agent_username': agent,
                'orders_count': len(data['unique_orders']),
                'unique_products_count': len(data['unique_products']),
                'total_items_quantity': data['items_count'],
                'last_invoice_date': data['last_invoice'].date() if hasattr(data['last_invoice'], 'date') else data['last_invoice']
            })
            if len(batch) >= 50:
                with BatchQuery() as b:
                    for item in batch:
                        CustomerLeaderboardAll.batch(b).create(**item)
                batch = []
    if batch:
        with BatchQuery() as b:
            for item in batch:
                CustomerLeaderboardAll.batch(b).create(**item)
    print(f"      Dodano {len([k for k,v in leaderboard.items() if v['items_count'] > 10])} klientow do leaderboarda")

    # Zamknij sesje
    cql_session.shutdown()
    cluster.shutdown()
    print("[Cassandra] Replikacja zakonczona!")


def main():
    print("=" * 60)
    print("REPLIKACJA DANYCH Z FIREBIRDA")
    print("=" * 60)
    print(f"MariaDB:   {'TAK' if REPLICATE_TO_MARIADB else 'NIE'}")
    print(f"OrientDB:  {'TAK' if REPLICATE_TO_ORIENTDB else 'NIE'}")
    print(f"Cassandra: {'TAK' if REPLICATE_TO_CASSANDRA else 'NIE'}")
    print(f"Drop przed replikacja: {'TAK' if DROP_BEFORE_REPLICATE else 'NIE'}")
    print("=" * 60)

    # Polacz z Firebirdem
    fb_session, fb_engine = create_firebird_session()

    # Pokaz statystyki
    print("\n[Firebird] Statystyki:")
    for model in [SysUser, Product, Customer, CustomerOrder, OrderItem, Invoice, Payment]:
        count = fb_session.query(model).count()
        print(f"   {model.__tablename__}: {count} rekordow")

    # ========== DROP DANYCH PRZED REPLIKACJA ==========
    if DROP_BEFORE_REPLICATE:
        print("\n" + "=" * 60)
        print("USUWANIE DANYCH Z DOCELOWYCH BAZ")
        print("=" * 60)

        if REPLICATE_TO_MARIADB:
            maria_session, maria_engine = create_mariadb_session()
            drop_mariadb_data(maria_session)
            maria_session.close()

        if REPLICATE_TO_ORIENTDB:
            try:
                orient_client = create_orient_connection()
                drop_orientdb_data(orient_client)
                orient_client.close()
            except Exception as e:
                print(f"[OrientDB] Blad przy usuwaniu danych: {e}")

        if REPLICATE_TO_CASSANDRA:
            try:
                drop_cassandra_data()
            except Exception as e:
                print(f"[Cassandra] Blad przy usuwaniu danych: {e}")

    # ========== REPLIKACJA ==========
    # Replikacja do MariaDB
    if REPLICATE_TO_MARIADB:
        maria_session, maria_engine = create_mariadb_session()
        maria_session = replicate_to_mariadb(fb_session, maria_session)
        maria_session.close()

    # Replikacja do OrientDB
    if REPLICATE_TO_ORIENTDB:
        replicate_to_orientdb(fb_session)

    # Replikacja do Cassandry
    if REPLICATE_TO_CASSANDRA:
        replicate_to_cassandra(fb_session)

    # Zamknij Firebird
    fb_session.close()
    print("\n" + "=" * 60)
    print("REPLIKACJA ZAKONCZONA!")
    print("=" * 60)


if __name__ == "__main__":
    main()
