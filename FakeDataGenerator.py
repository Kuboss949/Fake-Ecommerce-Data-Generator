import unicodedata
import csv
import random
from collections import defaultdict  # Do agregacji danych w pamięci
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy import inspect
from cassandra.cqlengine.query import BatchQuery
from cassandra_tables import (
    UsersByRole, CustomersByCity, ProductsByPrice,
    InvoiceFullDetails, PaymentsByYearAmount,
    SalesStatsByCountry, CustomerLeaderboard,
    InvoicesByCustomerName, OrderItemsByProduct,
    init_cassandra_schema
)

from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment
import pyorientdb as pyorient

class FakeDataGenerator:
    def __init__(self, main_session: Session, mirror_session: Session, orient_client):
        """
        Inicjalizacja generatora danych.

        Args:
            main_session: Sesja głównej bazy (Firebird)
            mirror_session: Sesja bazy lustrzanej (MariaDB)
            orient_client: Klient OrientDB
        """
        self.fake = Faker('pl_PL')
        self.session = main_session  # Główna sesja (Firebird)
        self.mirror_session = mirror_session  # Sesja lustrzana (MariaDB)
        self.orient_client = orient_client  # Klient OrientDB

        # --- AGREGATORY DANYCH DLA CASSANDRY ---
        # Cassandra nie robi "GROUP BY" wydajnie, więc policzymy to w Pythonie w trakcie generowania
        # Klucz: (kraj, nazwa_produktu), Wartość: ilość
        self.stats_sales_cache = defaultdict(int)

        # Klucz: customer_id, Wartość: słownik z danymi do leaderboarda
        self.stats_leaderboard_cache = {}

        # ... Twoje istniejące wagi ...
        self.USER_ROLES = ["SALES", "ACCOUNTANT", "WAREHOUSE"]
        self.USER_ROLE_WEIGHTS = [0.7, 0.1, 0.2]
        self.CUSTOMER_TYPE_WEIGHTS = [0.6, 0.4]
        self.ORDER_STATUS_RANDOM_WEIGHTS = [0.7, 0.3]
        self.INVOICE_STATUS_RANDOM_WEIGHTS = [30, 70]
        self.PAYMENT_METHODS = ['CREDIT CARD', 'PAYPAL', 'BANK TRANSFER', 'GOOGLE PAY']
        self.PAYMENT_METHOD_WEIGHTS = [40, 30, 20, 10]

    def remove_polish_chars(self, text):
        nfkd_form = unicodedata.normalize('NFKD', text)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    def generate_fake_products(self):
        products = []
        cassandra_batch = []
        print("Generowanie produktów i wysyłka do Cassandry...")
        with open('products_5k.csv', mode='r', encoding="utf-8") as file:
            csvFile = csv.reader(file)
            for i, lines in enumerate(csvFile):
                # SQL Object
                product = Product(
                    NAME=lines[0],
                    DESCRIPTION=lines[1].replace('"', ''),
                    PRICE=round(self.fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
                    STOCK_QUANTITY=random.randint(1, 500),
                )
                products.append(product)

                # --- CASSANDRA WRITE (BATCHED) ---
                cassandra_batch.append({
                    'bucket': "all_products",
                    'price': product.PRICE,
                    'product_id': i + 1,
                    'name': product.NAME,
                    'stock_quantity': product.STOCK_QUANTITY
                })

                # Flush batch every 500 records
                if len(cassandra_batch) >= 500:
                    with BatchQuery() as b:
                        for item in cassandra_batch:
                            ProductsByPrice.batch(b).create(**item)
                    cassandra_batch = []

            # Flush remaining records
            if cassandra_batch:
                with BatchQuery() as b:
                    for item in cassandra_batch:
                        ProductsByPrice.batch(b).create(**item)

        print(f"   -> Dodano {len(products)} produktów")
        return products

    def generate_fake_users(self, count: int):
        users = []
        cassandra_batch = []
        print("Generowanie userów i wysyłka do Cassandry...")
        for i in range(count):
            name = self.fake.first_name()
            surname = self.fake.last_name()
            username = self.remove_polish_chars(name) + self.remove_polish_chars(surname) + str(random.randint(1, 100))
            role = random.choices(self.USER_ROLES, weights=self.USER_ROLE_WEIGHTS, k=1)[0]

            user = SysUser(
                USERNAME=username,
                PASSWORD_HASH=self.fake.sha256(),
                NAME=name,
                SURNAME=surname,
                EMAIL=username + '@mycompany.com',
                ROLE=role
            )
            users.append(user)

            # --- CASSANDRA WRITE (BATCHED) ---
            cassandra_batch.append({
                'role': role,
                'user_id': i + 1,
                'username': username,
                'name': f"{name} {surname}",
                'email': user.EMAIL
            })

            # Flush batch every 500 records
            if len(cassandra_batch) >= 500:
                with BatchQuery() as b:
                    for item in cassandra_batch:
                        UsersByRole.batch(b).create(**item)
                cassandra_batch = []

        # Flush remaining records
        if cassandra_batch:
            with BatchQuery() as b:
                for item in cassandra_batch:
                    UsersByRole.batch(b).create(**item)

        print(f"   -> Dodano {len(users)} użytkowników")
        return users

    def generate_fake_customers(self, count: int):
        customers = []
        cassandra_batch = []
        print("Generowanie klientów i wysyłka do Cassandry...")
        for i in range(count):
            # ... Twoja logika generowania ...
            if random.choices([0, 1], weights=self.CUSTOMER_TYPE_WEIGHTS, k=1)[0] == 0:
                name = self.fake.first_name() + ' ' + self.fake.last_name()
                email = name.replace(' ', '').lower() + '@customer.pl'
            else:
                name = self.fake.company()
                domain = name.lower().replace(" ", "").replace('.', "") + ".com"
                email = f"contact@{domain}"

            customer = Customer(
                NAME=name,
                EMAIL=email,
                PHONE=self.fake.phone_number(),
                ADDRESS=f"{self.fake.street_name()} {self.fake.building_number()}, {self.fake.postcode()}",
                CITY=self.fake.city(),
                COUNTRY="Polska",
            )
            customers.append(customer)

            # --- CASSANDRA WRITE (BATCHED) ---
            cassandra_batch.append({
                'city': customer.CITY,
                'customer_id': i + 1,
                'name': name,
                'email': email
            })

            # Flush batch every 500 records
            if len(cassandra_batch) >= 500:
                with BatchQuery() as b:
                    for item in cassandra_batch:
                        CustomersByCity.batch(b).create(**item)
                cassandra_batch = []

        # Flush remaining records
        if cassandra_batch:
            with BatchQuery() as b:
                for item in cassandra_batch:
                    CustomersByCity.batch(b).create(**item)

        print(f"   -> Dodano {len(customers)} klientów")
        return customers

    # ... funkcja generate_order_items bez zmian ...
    def generate_order_items(self, products):
        # (Twoja oryginalna funkcja)
        items = []
        count = random.randint(1, 15)
        rd_products = random.choices(products, k=count)
        for product in rd_products:
            # UWAGA: product.PRODUCT_ID w tym momencie może być None, jeśli nie było commita.
            # Musimy polegać na tym, że lista products ma indeksy odpowiadające ID.
            # Dla celów skryptu demo przyjmijmy, że products[idx] ma ID = idx + 1
            items.append(OrderItem(
                ORDER_ID=0,
                PRODUCT_ID=products.index(product) + 1,  # Hack na brak ID przed commitem
                QUANTITY=random.randint(1, 10),
                UNIT_PRICE=product.PRICE
            ))

        return items

    def generate_fake_order_data(self, customers, products, sales_users, current_order_id):
        # Dodałem current_order_id jako argument, żebyśmy mieli ID do Cassandry

        # ... Twoja logika generowania dat i statusu ...
        start_datetime = datetime(2022, 1, 1, 0, 0, 0)
        end_datetime = datetime(2025, 9, 30, 23, 59, 59)
        random_dt = self.fake.date_time_between(start_date=start_datetime, end_date=end_datetime)
        days_diff = (end_datetime - random_dt).days
        rd_status = random.choices([0, 1], weights=self.ORDER_STATUS_RANDOM_WEIGHTS, k=1)[0]

        if days_diff > 60:
            status = 'COMPLETED' if rd_status == 0 else 'CANCELED'
        elif days_diff < 3:
            status = 'PENDING'
        else:
            status = 'PROCESSING' if rd_status == 0 else 'IN_TRANSIT'

        order_items = self.generate_order_items(products)
        total_amount = sum(item.UNIT_PRICE * item.QUANTITY for item in order_items)

        # Wybieramy klienta
        customer = random.choice(customers)

        order = CustomerOrder(
            CUSTOMER_ID=customers.index(customer) + 1,  # Hack na ID
            ORDER_DATE=random_dt,
            STATUS=status,
            TOTAL_AMOUNT=total_amount,
        )
        order.order_items = order_items
        self.session.add(order)

        # --- CASSANDRA AGGREGATION (Sales Stats) ---
        # Jeśli zamówienie jest zrealizowane, dodajemy do statystyk
        if status == 'COMPLETED':
            for item in order_items:
                # Szukamy nazwy produktu (trochę wolne wyszukiwanie, w produkcji robimy to inaczej)
                # products ma indeksy przesunięte o 1 względem ID
                prod_name = products[item.PRODUCT_ID - 1].NAME

                # Klucz: (Kraj, Nazwa Produktu) -> Wartość: ilość
                self.stats_sales_cache[(customer.COUNTRY, prod_name)] += item.QUANTITY

        invoice_creator = random.choice(sales_users)

        # Przekazujemy klienta i items dalej, żeby nie szukać ich znowu
        self.generate_fake_invoice(order, invoice_creator, customer, current_order_id, invoice_creator.USERNAME)

    def generate_fake_invoice(self, order, user, customer_obj, order_id, agent_username):
        # ... Twoja logika ...
        invoice_number = f"FV/{order.ORDER_DATE.strftime('%Y%m%d')}/{order_id:06d}"
        issue_date = order.ORDER_DATE
        due_date = issue_date + timedelta(days=14)
        total_amount = order.TOTAL_AMOUNT

        if order.STATUS == 'COMPLETED':
            status = 'PAID'
        elif order.STATUS == 'CANCELED':
            status = 'CANCELED'
        elif order.STATUS == 'PENDING':
            status = 'UNPAID'
        else:
            status = random.choices(['UNPAID', 'PAID'], weights=self.INVOICE_STATUS_RANDOM_WEIGHTS, k=1)[0]

        created_by = user.USER_ID  # Tutaj uwaga, user.USER_ID może być pusty przed commitem

        invoice = Invoice(
            INVOICE_NUMBER=invoice_number,
            STATUS=status,
            CUSTOMER_ID=order.CUSTOMER_ID,
            ISSUE_DATE=issue_date,
            DUE_DATE=due_date,
            TOTAL_AMOUNT=total_amount,
            CREATED_BY=created_by,  # Hack na ID
        )
        order.invoices.append(invoice)

        payment_method = None
        payment_amount = 0.0
        payment_confirmed = False
        payment_date = None

        if status == 'PAID':
            payment_date = order.ORDER_DATE + timedelta(days=random.randint(0, 6))
            payment_method = random.choices(self.PAYMENT_METHODS, weights=self.PAYMENT_METHOD_WEIGHTS, k=1)[0]
            payment_amount = float(order.TOTAL_AMOUNT)  # rzutowanie na float dla bezpieczenstwa
            payment_confirmed = True

            payment = Payment(
                PAYMENT_DATE=payment_date,
                AMOUNT=order.TOTAL_AMOUNT,
                METHOD=payment_method,
                CONFIRMED=1
            )
            invoice.payments.append(payment)

            # --- CASSANDRA WRITE (Payments) ---
            PaymentsByYearAmount.create(
                year=payment_date.year,
                amount=payment_amount,
                payment_id=random.randint(1, 10000000),  # Fake ID
                method=payment_method,
                payment_date=payment_date,
                confirmed=True
            )

        # --- CASSANDRA WRITE (Invoice Full Details) ---
            # --- CASSANDRA WRITE (Invoice Full Details & Helpers) ---
            # Symulujemy ID faktury (w realu pobralibysmy po save)
            fake_invoice_id = random.randint(1, 10000000)

            # Używamy BatchQuery, żeby wysłać dane do 3 tabel
            with BatchQuery() as b:
                # 1. Główna tabela faktur
                InvoiceFullDetails.batch(b).create(
                    invoice_id=fake_invoice_id,
                    invoice_number=invoice_number,
                    issue_date=issue_date.date(),
                    due_date=due_date.date(),
                    total_amount=float(total_amount),
                    status=status,
                    past_due=False,
                    # Dane zdenormalizowane
                    customer_id=customer_obj.CUSTOMER_ID if hasattr(customer_obj, 'CUSTOMER_ID') else 0,
                    customer_name=customer_obj.NAME,
                    customer_email=customer_obj.EMAIL,
                    payment_method=payment_method if payment_method else "N/A",
                    payment_amount=payment_amount,
                    payment_confirmed=payment_confirmed
                )

                # 2. Tabela pomocnicza dla UPDATE (po nazwie klienta)
                InvoicesByCustomerName.batch(b).create(
                    customer_name=customer_obj.NAME,
                    invoice_id=fake_invoice_id,
                    total_amount=float(total_amount),
                    status=status
                )

                # 3. Tabela pomocnicza dla DELETE (po produkcie)
                # Iterujemy po przedmiotach z tego konkretnego zamówienia
                # order.order_items mamy dostępne, bo przekazałeś obiekt order do funkcji
                for item in order.order_items:
                    OrderItemsByProduct.batch(b).create(
                        product_id=item.PRODUCT_ID,
                        invoice_id=fake_invoice_id,
                        quantity=item.QUANTITY,
                        unit_price=float(item.UNIT_PRICE)
                    )

        # --- CASSANDRA AGGREGATION (Leaderboard) ---
        # Zbieramy dane do tabeli "Customer 360"
        # Aktualizujemy cache dla tego klienta
        c_key = customer_obj.CUSTOMER_ID  # Używamy ID jako klucza w słowniku pomocniczym

        if c_key not in self.stats_leaderboard_cache:
            self.stats_leaderboard_cache[c_key] = {
                'country': customer_obj.COUNTRY,
                'customer_name': customer_obj.NAME,
                'agent': agent_username,
                'gross_value': 0.0,
                'orders_count': 0,
                'items_count': 0,
                'unique_products': set(),  # Set żeby zliczyć unikalne
                'last_invoice': issue_date
            }

        # Aktualizujemy tylko jeśli zamówienie jest zakończone i opłacone (zgodnie z logiką SQL)
        if order.STATUS == 'COMPLETED' and payment_confirmed:
            stats = self.stats_leaderboard_cache[c_key]
            stats['gross_value'] += float(total_amount)
            stats['orders_count'] += 1
            stats['items_count'] += sum(i.QUANTITY for i in order.order_items)
            for i in order.order_items:
                stats['unique_products'].add(i.PRODUCT_ID)

            if issue_date > stats['last_invoice']:
                stats['last_invoice'] = issue_date

    def flush_cassandra_stats(self):
        print("Finalizowanie: Zapisywanie zagregowanych statystyk do Cassandry...")

        # 1. Zapis Sales Stats (BATCHED)
        total_sales = len(self.stats_sales_cache)
        print(f"Zapisywanie {total_sales} rekordów sprzedaży...")
        sales_batch = []
        processed = 0

        for (country, prod_name), qty in self.stats_sales_cache.items():
            sales_batch.append({
                'country': country,
                'product_name': prod_name,
                'total_quantity_sum': qty,
                'product_id': 0
            })

            if len(sales_batch) >= 500:
                with BatchQuery() as b:
                    for item in sales_batch:
                        SalesStatsByCountry.batch(b).create(**item)
                processed += len(sales_batch)
                if processed % 5000 == 0:
                    print(f"   ... {processed}/{total_sales}")
                sales_batch = []

        # Flush remaining
        if sales_batch:
            with BatchQuery() as b:
                for item in sales_batch:
                    SalesStatsByCountry.batch(b).create(**item)

        # 2. Zapis Leaderboard (BATCHED)
        total_leaders = len(self.stats_leaderboard_cache)
        print(f"Zapisywanie leaderboarda dla {total_leaders} klientów...")
        leader_batch = []

        for cid, data in self.stats_leaderboard_cache.items():
            # Filtrujemy tylko tych co coś kupili (opcjonalne)
            if data['orders_count'] > 0:
                leader_batch.append({
                    'country': data['country'],
                    'gross_value_brutto': data['gross_value'],
                    'customer_name': data['customer_name'],
                    'agent_username': data['agent'],
                    'orders_count': data['orders_count'],
                    'unique_products_count': len(data['unique_products']),
                    'total_items_quantity': data['items_count'],
                    'last_invoice_date': data['last_invoice'].date()
                })

                if len(leader_batch) >= 500:
                    with BatchQuery() as b:
                        for item in leader_batch:
                            CustomerLeaderboard.batch(b).create(**item)
                    leader_batch = []

        # Flush remaining
        if leader_batch:
            with BatchQuery() as b:
                for item in leader_batch:
                    CustomerLeaderboard.batch(b).create(**item)

        print("   -> Statystyki zapisane.")

    def replicate_sql_data(self, target_session, batch_size=2000):
        """Kopiuje dane z bieżącej sesji (Firebird) do sesji docelowej (MariaDB) ze streamingiem"""
        print("\n Rozpoczynam replikację danych (Firebird -> MariaDB)...")

        models_order = [
            SysUser,
            Product,
            Customer,
            CustomerOrder,
            OrderItem,
            Invoice,
            Payment
        ]

        try:
            for model in models_order:
                model_name = model.__tablename__
                column_attrs = list(inspect(model).mapper.column_attrs)

                # Count first (fast query)
                total_count = self.session.query(model).count()
                print(f"   -> Kopiowanie tabeli: {model_name} ({total_count} rekordów)...")

                if total_count == 0:
                    continue

                # Stream data using yield_per to avoid loading all into memory
                target_objects_buffer = []
                processed = 0

                for obj in self.session.query(model).yield_per(batch_size):
                    data = {c.key: getattr(obj, c.key) for c in column_attrs}
                    target_objects_buffer.append(model(**data))
                    processed += 1

                    # Flush batch when full
                    if len(target_objects_buffer) >= batch_size:
                        try:
                            target_session.bulk_save_objects(target_objects_buffer)
                            target_session.commit()
                            if processed % 10000 == 0:
                                print(f"      ... {processed}/{total_count}")
                        except Exception as chunk_error:
                            print(f"Błąd przy zapisie paczki dla {model_name}: {chunk_error}")
                            target_session.rollback()
                            raise chunk_error
                        target_objects_buffer = []

                # Flush remaining objects
                if target_objects_buffer:
                    try:
                        target_session.bulk_save_objects(target_objects_buffer)
                        target_session.commit()
                    except Exception as chunk_error:
                        print(f"Błąd przy zapisie ostatniej paczki dla {model_name}: {chunk_error}")
                        target_session.rollback()
                        raise chunk_error

                # Clear session cache to free memory after each table
                self.session.expire_all()

            print("Replikacja zakończona sukcesem!")

        except Exception as e:
            print(f"Błąd krytyczny podczas replikacji: {e}")
            target_session.rollback()
            raise e

    def run_generation(self, num_users: int, num_customers: int, num_orders: int):
        print("Start: Generowanie danych...")

        # --- FAZA 1: Produkty, Użytkownicy, Klienci ---
        products = self.generate_fake_products()
        self.session.bulk_save_objects(products, return_defaults=True)
        # Hack: nadajemy tymczasowe ID, żeby logika działała przed commit
        for i, p in enumerate(products): p.PRODUCT_ID = i + 1

        users = self.generate_fake_users(num_users)
        self.session.bulk_save_objects(users, return_defaults=True)
        # Hack ID
        for i, u in enumerate(users): u.USER_ID = i + 1

        customers = self.generate_fake_customers(num_customers)
        self.session.bulk_save_objects(customers, return_defaults=True)
        # Hack ID
        for i, c in enumerate(customers): c.CUSTOMER_ID = i + 1

        # Commit base data first to reduce transaction size
        print("Commit bazy (produkty, użytkownicy, klienci)...")
        self.session.commit()
        print("   -> Baza zapisana.")

        sales_users = [u for u in users if u.ROLE == 'SALES']

        # --- FAZA 2: Zamówienia z pośrednimi commitami ---
        print(f"Start: Generowanie {num_orders} zamówień...")
        commit_interval = 5000  # Commit every 5000 orders to balance performance and safety

        for i in range(num_orders):
            # Przekazujemy i + 1 jako ID zamówienia
            self.generate_fake_order_data(customers, products, sales_users, i + 1)

            # Progress report
            if (i + 1) % 1000 == 0:
                print(f" ... wygenerowano {i + 1}/{num_orders}")

            # Intermediate commit to reduce memory and transaction size
            if (i + 1) % commit_interval == 0:
                print(f"   -> Pośredni commit ({i + 1} zamówień)...")
                self.session.commit()

        # Final commit for remaining orders
        print("   -> Finalny commit zamówień...")
        self.session.commit()

        # --- FINALIZACJA CASSANDRY ---
        self.flush_cassandra_stats()

        try:
            print("Gotowe - Firebird zapisany.")

            self.replicate_sql_data(target_session=self.mirror_session)

            # --- UPLOAD DO ORIENTDB (ZOPTYMALIZOWANY) ---
            self._replicate_to_orientdb()

        except Exception as e:
            print(f"❌ BŁĄD: {e}")
            self.session.rollback()
            # mirror_session rollback jest robiony wewnątrz funkcji replicate
            raise e  # Re-raise żeby zobaczyć pełny traceback
        # finally:
        #     self.session.close()
        #     self.mirror_session.close()
        #     self.orient_client.db_close()
        #     print("🔒 Wszystkie połączenia zamknięte.")

    def _refresh_orient_connection(self):
        """Odświeża połączenie z OrientDB, aby uniknąć wygaśnięcia tokenu sesji."""
        try:
            try:
                self.orient_client.db_close()
            except:
                pass
            try:
                self.orient_client.close()
            except:
                pass
        except:
            pass

        self.orient_client = pyorient.OrientDB("localhost", 2424)
        self.orient_client.connect("root", "root")
        self.orient_client.db_open("company", "root", "root")

    def _escape_orient_string(self, value):
        """Escapuje znaki specjalne w stringach dla OrientDB."""
        if value is None:
            return ""
        return str(value).replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')

    def _batch_insert_orient(self, table_name, records, batch_size=500):
        """
        Wstawia rekordy do OrientDB w partiach używając batch script.
        Zwraca słownik mapujący ID na RID dla późniejszego tworzenia krawędzi.
        """
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
                        fields.append(f"{key} = '{self._escape_orient_string(value)}'")
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
                results = self.orient_client.batch(batch_script)
                # Mapujemy ID na RID
                for idx, record in enumerate(batch):
                    # Szukamy klucza ID (CUSTOMER_ID, ORDER_ID, etc.)
                    id_key = None
                    for k in record.keys():
                        if k.endswith("_ID") and k != "CUSTOMER_ID" or k == table_name.replace("CUSTOMER_", "") + "_ID":
                            id_key = k
                            break
                    if id_key is None:
                        # Próbujemy standardowe nazwy
                        possible_keys = [f"{table_name}_ID", "CUSTOMER_ID", "ORDER_ID", "USER_ID",
                                         "PRODUCT_ID", "INVOICE_ID", "PAYMENT_ID", "ORDER_ITEM_ID"]
                        for pk in possible_keys:
                            if pk in record:
                                id_key = pk
                                break

                    if id_key and idx < len(results):
                        try:
                            rid = results[idx]._rid if hasattr(results[idx], '_rid') else str(results[idx])
                            id_to_rid[record[id_key]] = rid
                        except:
                            pass
            except Exception as e:
                # Fallback do pojedynczych insertów jeśli batch zawiedzie
                print(f"      [WARN] Batch insert failed, falling back to single inserts: {e}")
                for record in batch:
                    try:
                        fields = []
                        for key, value in record.items():
                            if isinstance(value, str):
                                fields.append(f"{key} = '{self._escape_orient_string(value)}'")
                            elif value is None:
                                fields.append(f"{key} = null")
                            else:
                                fields.append(f"{key} = {value}")
                        fields_str = ", ".join(fields)
                        result = self.orient_client.command(f"INSERT INTO {table_name} SET {fields_str}")
                        if result and len(result) > 0:
                            rid = result[0]._rid if hasattr(result[0], '_rid') else str(result[0])
                            # Znajdź klucz ID
                            for k in record.keys():
                                if k.endswith("_ID"):
                                    id_to_rid[record[k]] = rid
                                    break
                    except Exception as insert_err:
                        pass

            inserted += len(batch)
            if inserted % 5000 == 0 or inserted == total:
                print(f"      ... {inserted}/{total} rekordów")

            # Odświeżamy połączenie co 10000 rekordów
            if inserted % 10000 == 0 and inserted < total:
                print("      -> Odświeżanie połączenia...")
                self._refresh_orient_connection()

        return id_to_rid

    def _replicate_to_orientdb(self):
        """Zoptymalizowana replikacja do OrientDB z batch insertami i cache'owaniem RID."""
        print("\n Rozpoczynam replikację do OrientDB...")
        print("   -> Nawiązywanie świeżego połączenia z OrientDB...")
        try:
            self._refresh_orient_connection()
            print("   -> Nowe połączenie aktywne!")
        except Exception as e:
            print(f"BLAD: Nie udało się nawiązać połączenia: {e}")
            raise e

        # Cache dla RID - pozwala na szybkie tworzenie krawędzi bez dodatkowych SELECT
        rid_cache = {
            'customer': {},    # CUSTOMER_ID -> RID
            'user': {},        # USER_ID -> RID
            'product': {},     # PRODUCT_ID -> RID
            'order': {},       # ORDER_ID -> RID
            'order_item': {},  # ORDER_ITEM_ID -> RID
            'invoice': {},     # INVOICE_ID -> RID
            'payment': {},     # PAYMENT_ID -> RID
        }

        # Mapowania dla krawędzi (klucz obcy -> lista RID)
        fk_mappings = {
            'invoice_by_customer': defaultdict(list),    # CUSTOMER_ID -> [Invoice RIDs]
            'invoice_by_order': defaultdict(list),       # ORDER_ID -> [Invoice RIDs]
            'invoice_by_user': defaultdict(list),        # CREATED_BY -> [Invoice RIDs]
            'order_by_customer': defaultdict(list),      # CUSTOMER_ID -> [Order RIDs]
            'order_item_by_order': defaultdict(list),    # ORDER_ID -> [OrderItem RIDs]
            'order_item_by_product': defaultdict(list),  # PRODUCT_ID -> [OrderItem RIDs]
            'payment_by_invoice': defaultdict(list),     # INVOICE_ID -> [Payment RIDs]
        }

        # 1. CUSTOMERS
        print("   -> Dodawanie Customers...")
        customers = self.session.execute(select(Customer)).scalars().all()
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
        rid_cache['customer'] = self._batch_insert_orient('CUSTOMER', customer_records)
        print(f"      Dodano {len(customers)} klientów")

        # 2. USERS
        print("   -> Dodawanie Users...")
        self._refresh_orient_connection()
        users = self.session.execute(select(SysUser)).scalars().all()
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
        rid_cache['user'] = self._batch_insert_orient('SYS_USER', user_records)
        print(f"      Dodano {len(users)} użytkowników")

        # 3. PRODUCTS
        print("   -> Dodawanie Products...")
        self._refresh_orient_connection()
        products = self.session.execute(select(Product)).scalars().all()
        product_records = [
            {
                'PRODUCT_ID': p.PRODUCT_ID,
                'NAME': p.NAME,
                'DESCRIPTION': p.DESCRIPTION,
                'PRICE': round(float(p.PRICE), 2),
                'STOCK_QUANTITY': p.STOCK_QUANTITY
            } for p in products
        ]
        rid_cache['product'] = self._batch_insert_orient('PRODUCT', product_records)
        print(f"      Dodano {len(products)} produktów")

        # 4. ORDERS
        print("   -> Dodawanie Orders...")
        self._refresh_orient_connection()
        orders = self.session.execute(select(CustomerOrder)).scalars().all()
        order_records = [
            {
                'ORDER_ID': o.ORDER_ID,
                'CUSTOMER_ID': o.CUSTOMER_ID,
                'ORDER_DATE': str(o.ORDER_DATE),
                'STATUS': o.STATUS,
                'TOTAL_AMOUNT': round(float(o.TOTAL_AMOUNT), 2)
            } for o in orders
        ]
        rid_cache['order'] = self._batch_insert_orient('CUSTOMER_ORDER', order_records)
        # Mapujemy CUSTOMER_ID -> Order RIDs dla krawędzi
        for o in orders:
            if o.ORDER_ID in rid_cache['order']:
                fk_mappings['order_by_customer'][o.CUSTOMER_ID].append(rid_cache['order'][o.ORDER_ID])
        print(f"      Dodano {len(orders)} zamówień")

        # 5. ORDER ITEMS
        print("   -> Dodawanie Order Items...")
        self._refresh_orient_connection()
        order_items = self.session.execute(select(OrderItem)).scalars().all()
        order_item_records = [
            {
                'ORDER_ITEM_ID': oi.ORDER_ITEM_ID,
                'ORDER_ID': oi.ORDER_ID,
                'PRODUCT_ID': oi.PRODUCT_ID,
                'QUANTITY': oi.QUANTITY,
                'UNIT_PRICE': round(float(oi.UNIT_PRICE), 2)
            } for oi in order_items
        ]
        rid_cache['order_item'] = self._batch_insert_orient('ORDER_ITEM', order_item_records)
        # Mapowania dla krawędzi
        for oi in order_items:
            if oi.ORDER_ITEM_ID in rid_cache['order_item']:
                rid = rid_cache['order_item'][oi.ORDER_ITEM_ID]
                fk_mappings['order_item_by_order'][oi.ORDER_ID].append(rid)
                fk_mappings['order_item_by_product'][oi.PRODUCT_ID].append(rid)
        print(f"      Dodano {len(order_items)} pozycji zamówień")

        # 6. INVOICES
        print("   -> Dodawanie Invoices...")
        self._refresh_orient_connection()
        invoices = self.session.execute(select(Invoice)).scalars().all()
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
        rid_cache['invoice'] = self._batch_insert_orient('INVOICE', invoice_records)
        # Mapowania dla krawędzi
        for i in invoices:
            if i.INVOICE_ID in rid_cache['invoice']:
                rid = rid_cache['invoice'][i.INVOICE_ID]
                fk_mappings['invoice_by_customer'][i.CUSTOMER_ID].append(rid)
                fk_mappings['invoice_by_order'][i.ORDER_ID].append(rid)
                fk_mappings['invoice_by_user'][i.CREATED_BY].append(rid)
        print(f"      Dodano {len(invoices)} faktur")

        # 7. PAYMENTS
        print("   -> Dodawanie Payments...")
        self._refresh_orient_connection()
        payments = self.session.execute(select(Payment)).scalars().all()
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
        rid_cache['payment'] = self._batch_insert_orient('PAYMENT', payment_records)
        # Mapowanie dla krawędzi
        for p in payments:
            if p.PAYMENT_ID in rid_cache['payment']:
                fk_mappings['payment_by_invoice'][p.INVOICE_ID].append(rid_cache['payment'][p.PAYMENT_ID])
        print(f"      Dodano {len(payments)} płatności")

        # --- TWORZENIE KRAWĘDZI (ZOPTYMALIZOWANE) ---
        print("   -> Tworzenie krawędzi (edges)...")
        self._refresh_orient_connection()

        self._create_edges_batch('Customer_to_invoice', rid_cache['customer'], fk_mappings['invoice_by_customer'])
        self._create_edges_batch('Invoice_to_payment', rid_cache['invoice'], fk_mappings['payment_by_invoice'])
        self._create_edges_batch('Customer_to_order', rid_cache['customer'], fk_mappings['order_by_customer'])
        self._create_edges_batch('Order_to_invoice', rid_cache['order'], fk_mappings['invoice_by_order'])
        self._create_edges_batch('User_to_invoice', rid_cache['user'], fk_mappings['invoice_by_user'])
        self._create_edges_batch('Order_to_order_item', rid_cache['order'], fk_mappings['order_item_by_order'])
        self._create_edges_batch('Product_to_order_item', rid_cache['product'], fk_mappings['order_item_by_product'])

        print("Replikacja do OrientDB zakończona sukcesem!")

    def _create_edges_batch(self, edge_class, from_rid_map, to_rids_by_fk, batch_size=200):
        """
        Tworzy krawędzie w partiach używając bezpośrednich RID zamiast SELECT.
        from_rid_map: {ID -> RID} dla wierzchołków źródłowych
        to_rids_by_fk: {FK_ID -> [RID, ...]} dla wierzchołków docelowych
        """
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
            print(f"         Brak krawędzi do utworzenia")
            return

        for i in range(0, total_edges, batch_size):
            batch = edge_commands[i:i + batch_size]

            # Budujemy batch script dla krawędzi
            batch_script = "begin\n"
            for idx, (from_rid, to_rid) in enumerate(batch):
                batch_script += f"CREATE EDGE {edge_class} FROM {from_rid} TO {to_rid}\n"
            batch_script += "commit\nreturn true"

            try:
                self.orient_client.batch(batch_script)
                edges_created += len(batch)
            except Exception as e:
                # Fallback do pojedynczych insertów
                for from_rid, to_rid in batch:
                    try:
                        self.orient_client.command(f"CREATE EDGE {edge_class} FROM {from_rid} TO {to_rid}")
                        edges_created += 1
                    except:
                        pass

            if edges_created % 5000 == 0 and edges_created > 0:
                print(f"         ... {edges_created}/{total_edges}")
                self._refresh_orient_connection()

        print(f"         Utworzono {edges_created} krawędzi")
