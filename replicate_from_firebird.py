"""
Skrypt do replikacji danych z Firebirda do MariaDB, OrientDB i Cassandry.
Uruchamiany osobno, gdy generowanie danych zostalo zakonczone, ale replikacja sie nie powiodla.

UWAGA: Ten skrypt NIE usuwa danych z zadnej bazy - tylko dodaje/aktualizuje.
"""

import sys
from collections import defaultdict
from sqlalchemy import create_engine, select, inspect
from sqlalchemy.orm import sessionmaker
import pyorientdb as pyorient

from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment
from cassandra_tables import (
    UsersByRole, CustomersByCity, ProductsByPrice,
    InvoiceFullDetails, PaymentsByYearAmount,
    SalesStatsByCountry, CustomerLeaderboard,
    InvoicesByCustomerName, OrderItemsByProduct,
    init_cassandra_schema
)
from cassandra.cqlengine.query import BatchQuery


# ============== KONFIGURACJA ==============
FIREBIRD_URL = "firebird+firebird://SYSDBA:masterkey@localhost:3050//firebird/data/company.fdb?charset=WIN1250"
MARIADB_URL = "mysql+pymysql://root:root@localhost:3306/company?charset=utf8mb4"
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
REPLICATE_TO_CASSANDRA = False  # Cassandra juz ma dane z generacji


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

        # Sprawdz ile juz jest w MariaDB
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

    # Sprawdz czy dane juz istnieja
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


def main():
    print("=" * 60)
    print("REPLIKACJA DANYCH Z FIREBIRDA")
    print("=" * 60)
    print(f"MariaDB:   {'TAK' if REPLICATE_TO_MARIADB else 'NIE'}")
    print(f"OrientDB:  {'TAK' if REPLICATE_TO_ORIENTDB else 'NIE'}")
    print(f"Cassandra: {'TAK' if REPLICATE_TO_CASSANDRA else 'NIE'}")
    print("=" * 60)

    # Polacz z Firebirdem
    fb_session, fb_engine = create_firebird_session()

    # Pokaz statystyki
    print("\n[Firebird] Statystyki:")
    for model in [SysUser, Product, Customer, CustomerOrder, OrderItem, Invoice, Payment]:
        count = fb_session.query(model).count()
        print(f"   {model.__tablename__}: {count} rekordow")

    # Replikacja do MariaDB
    if REPLICATE_TO_MARIADB:
        maria_session, maria_engine = create_mariadb_session()
        maria_session = replicate_to_mariadb(fb_session, maria_session)
        maria_session.close()

    # Replikacja do OrientDB
    if REPLICATE_TO_ORIENTDB:
        replicate_to_orientdb(fb_session)

    # Zamknij Firebird
    fb_session.close()
    print("\n" + "=" * 60)
    print("REPLIKACJA ZAKONCZONA!")
    print("=" * 60)


if __name__ == "__main__":
    main()
