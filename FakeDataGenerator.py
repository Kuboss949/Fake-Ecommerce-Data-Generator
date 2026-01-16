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
        print("Generowanie produktów i wysyłka do Cassandry...")
        with open('products.csv', mode='r', encoding="utf-8") as file:
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

                # --- CASSANDRA WRITE ---
                # Zakładamy ID = i + 1, bo baza SQL jeszcze nie nadała ID (chyba że zrobisz flush)
                # Dla uproszczenia przyjmuję, że ID będą zgodne z kolejnością
                ProductsByPrice.create(
                    bucket="all_products",  # Stała wartość do partycjonowania
                    price=product.PRICE,
                    product_id=i + 1,  # Symulujemy ID
                    name=product.NAME,
                    stock_quantity=product.STOCK_QUANTITY
                )
        return products

    def generate_fake_users(self, count: int):
        users = []
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

            # --- CASSANDRA WRITE ---
            UsersByRole.create(
                role=role,
                user_id=i + 1,  # Symulacja ID
                username=username,
                name=f"{name} {surname}",
                email=user.EMAIL
            )
        return users

    def generate_fake_customers(self, count: int):
        customers = []
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

            # --- CASSANDRA WRITE ---
            CustomersByCity.create(
                city=customer.CITY,
                customer_id=i + 1,
                name=name,
                email=email
            )
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
        random_suffix = random.randint(100000, 999999)
        invoice_number = f"FV/{order.ORDER_DATE.strftime('%Y%m%d')}/{random_suffix}"
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

        # 1. Zapis Sales Stats
        print(f"Zapisywanie {len(self.stats_sales_cache)} rekordów sprzedaży...")
        for (country, prod_name), qty in self.stats_sales_cache.items():
            SalesStatsByCountry.create(
                country=country,
                product_name=prod_name,
                total_quantity_sum=qty,
                product_id=0  # Opcjonalne
            )

        # 2. Zapis Leaderboard
        print(f"Zapisywanie leaderboarda dla {len(self.stats_leaderboard_cache)} klientów...")
        for cid, data in self.stats_leaderboard_cache.items():
            # Filtrujemy tylko tych co coś kupili (opcjonalne)
            if data['orders_count'] > 0:
                CustomerLeaderboard.create(
                    country=data['country'],
                    gross_value_brutto=data['gross_value'],
                    customer_name=data['customer_name'],
                    agent_username=data['agent'],
                    orders_count=data['orders_count'],
                    unique_products_count=len(data['unique_products']),
                    total_items_quantity=data['items_count'],
                    last_invoice_date=data['last_invoice'].date()
                )

    def replicate_sql_data(self, target_session):
        """Kopiuje dane z bieżącej sesji (Firebird) do sesji docelowej (MariaDB)"""
        print("\n🔄 Rozpoczynam replikację danych (Firebird -> MariaDB)...")

        # Kolejność tabel jest WAŻNA (od rodzica do dziecka), żeby nie naruszyć Kluczy Obcych
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
                # 1. Pobieramy dane z Firebirda (self.session)
                source_objects = self.session.query(model).all()
                count = len(source_objects)
                print(f"   ➡️ Kopiowanie tabeli: {model_name} ({count} rekordów)...")

                target_objects = []
                for obj in source_objects:
                    # 2. Klonujemy obiekt "na czysto" wyciągając dane do słownika
                    # inspect().mapper.column_attrs daje nam listę kolumn z modelu
                    data = {c.key: getattr(obj, c.key) for c in inspect(model).mapper.column_attrs}

                    # Tworzymy nową instancję dla MariaDB
                    new_obj = model(**data)
                    target_objects.append(new_obj)

                # 3. Zapisujemy paczkę do MariaDB
                if target_objects:
                    target_session.bulk_save_objects(target_objects)
                    # Commitujemy po każdej tabeli, żeby ID były widoczne dla następnych tabel (FK)
                    target_session.commit()

            print("✅ Replikacja zakończona sukcesem!")

        except Exception as e:
            print(f"❌ Błąd podczas replikacji: {e}")
            target_session.rollback()
            raise e

    def run_generation(self, num_users: int, num_customers: int, num_orders: int):
        print("Start: Generowanie danych...")

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

        sales_users = [u for u in users if u.ROLE == 'SALES']

        print(f"Start: Generowanie {num_orders} zamówień...")
        for i in range(num_orders):
            # Przekazujemy i + 1 jako ID zamówienia
            self.generate_fake_order_data(customers, products, sales_users, i + 1)
            if (i + 1) % 100 == 0:
                print(f" ... wygenerowano {i + 1}/{num_orders}")

        # --- FINALIZACJA CASSANDRY ---
        self.flush_cassandra_stats()

        try:
            print("Commit SQL (Firebird)...")
            self.session.commit()  # Zapisujemy w Firebirdzie
            print("Gotowe - Firebird zapisany.")

            self.replicate_sql_data(target_session=self.mirror_session)

            # --- UPLOAD DO ORIENTDB ---
            print("\n📊 Rozpoczynam replikację do OrientDB...")

            print("   -> Dodawanie Customers...")
            orient_customers = self.session.execute(select(Customer))
            orient_customers_scalar = orient_customers.scalars().all()
            for c in orient_customers_scalar:
                customer_make = "insert into CUSTOMER set CUSTOMER_ID =  %d, NAME =  '%s', EMAIL = '%s' ,PHONE = '%s', ADDRESS = '%s', CITY = '%s', COUNTRY = '%s'"\
                % (c.CUSTOMER_ID, c.NAME, c.EMAIL, c.PHONE, c.ADDRESS, c.CITY, c.COUNTRY)
                self.orient_client.command(customer_make)

            print("   -> Dodawanie Users...")
            orient_users = self.session.execute(select(SysUser))
            orient_users_scalar = orient_users.scalars().all()
            for u in orient_users_scalar:
                user_make = "insert into SYS_USER set USER_ID =  %d, USERNAME = '%s' ,PASSWORD_HASH = '%s', NAME = '%s', SURNAME = '%s', EMAIL = '%s', ROLE = '%s', ACTIVE = '%s'"\
                % (u.USER_ID, u.USERNAME, u.PASSWORD_HASH, u.NAME, u.SURNAME, u.EMAIL, u.ROLE, u.ACTIVE)
                self.orient_client.command(user_make)

            print("   -> Dodawanie Products...")
            orient_products = self.session.execute(select(Product))
            orient_products_scalar = orient_products.scalars().all()
            for p in orient_products_scalar:
                product_make = "insert into PRODUCT set PRODUCT_ID =  %d, NAME = '%s' ,DESCRIPTION = '%s', PRICE = %06.2f, STOCK_QUANTITY = %d" \
                % (p.PRODUCT_ID, p.NAME, p.DESCRIPTION, p.PRICE, p.STOCK_QUANTITY)
                self.orient_client.command(product_make)

            print("   -> Dodawanie Orders...")
            orient_customer_orders = self.session.execute(select(CustomerOrder))
            orient_customer_orders_scalar = orient_customer_orders.scalars().all()
            for c in orient_customer_orders_scalar:
                orders_make = "insert into CUSTOMER_ORDER set ORDER_ID =  %d, CUSTOMER_ID = %d ,ORDER_DATE = '%s', STATUS = '%s', TOTAL_AMOUNT = %f" \
                % (c.ORDER_ID, c.CUSTOMER_ID, c.ORDER_DATE, c.STATUS, c.TOTAL_AMOUNT)
                self.orient_client.command(orders_make)

            print("   -> Dodawanie Order Items...")
            orient_order_items = self.session.execute(select(OrderItem))
            orient_order_items_scalar = orient_order_items.scalars().all()
            for o in orient_order_items_scalar:
                order_item_make = "insert into ORDER_ITEM set ORDER_ITEM_ID = %d, ORDER_ID = %d, PRODUCT_ID = %d, QUANTITY = %d, UNIT_PRICE = %f" \
                % (o.ORDER_ITEM_ID, o.ORDER_ID, o.PRODUCT_ID, o.QUANTITY, o.UNIT_PRICE)
                self.orient_client.command(order_item_make)

            print("   -> Dodawanie Invoices...")
            orient_invoice = self.session.execute(select(Invoice))
            orient_invoice_scalar = orient_invoice.scalars().all()
            for i in orient_invoice_scalar:
                invoice_make = "insert into INVOICE set INVOICE_ID = %d, INVOICE_NUMBER =  '%s', CUSTOMER_ID = %d, ORDER_ID = %d, STATUS = '%s',  ISSUE_DATE = '%s', DUE_DATE = '%s', TOTAL_AMOUNT = %f, CREATED_BY = %d" \
                % (i.INVOICE_ID, i.INVOICE_NUMBER, i.CUSTOMER_ID, i.ORDER_ID, i.STATUS, i.ISSUE_DATE, i.DUE_DATE, i.TOTAL_AMOUNT, i.CREATED_BY)
                self.orient_client.command(invoice_make)

            print("   -> Dodawanie Payments...")
            orient_payment = self.session.execute(select(Payment))
            orient_payment_scalar = orient_payment.scalars().all()
            for p in orient_payment_scalar:
                payment_make = "insert into PAYMENT set PAYMENT_ID =  %d , INVOICE_ID =  %d ,PAYMENT_DATE = '%s', AMOUNT = %f, METHOD = '%s', CONFIRMED = %d" \
                % (p.PAYMENT_ID, p.INVOICE_ID, p.PAYMENT_DATE, p.AMOUNT, p.METHOD, p.CONFIRMED)
                self.orient_client.command(payment_make)

            print("   -> Tworzenie krawędzi (edges)...")

            # Customer -> Invoice
            print("      -> Customer_to_invoice...")
            customers_result = self.orient_client.command("SELECT FROM CUSTOMER")
            for c in customers_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Customer_to_invoice FROM (SELECT FROM CUSTOMER WHERE CUSTOMER_ID = {c.oRecordData['CUSTOMER_ID']}) TO (SELECT FROM INVOICE WHERE CUSTOMER_ID = {c.oRecordData['CUSTOMER_ID']})"
                    )
                except:
                    # Klient może nie mieć faktur, pomijamy
                    pass

            # Invoice -> Payment
            print("      -> Invoice_to_payment...")
            invoices_result = self.orient_client.command("SELECT FROM INVOICE")
            for i in invoices_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Invoice_to_payment FROM (SELECT FROM INVOICE WHERE INVOICE_ID = {i.oRecordData['INVOICE_ID']}) TO (SELECT FROM PAYMENT WHERE INVOICE_ID = {i.oRecordData['INVOICE_ID']})"
                    )
                except:
                    # Faktura może nie mieć płatności, pomijamy
                    pass

            # Customer -> Order
            print("      -> Customer_to_order...")
            customers_result = self.orient_client.command("SELECT FROM CUSTOMER")
            for c in customers_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Customer_to_order FROM (SELECT FROM CUSTOMER WHERE CUSTOMER_ID = {c.oRecordData['CUSTOMER_ID']}) TO (SELECT FROM CUSTOMER_ORDER WHERE CUSTOMER_ID = {c.oRecordData['CUSTOMER_ID']})"
                    )
                except:
                    # Klient może nie mieć zamówień, pomijamy
                    pass

            # Order -> Invoice
            print("      -> Order_to_invoice...")
            orders_result = self.orient_client.command("SELECT FROM CUSTOMER_ORDER")
            for o in orders_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Order_to_invoice FROM (SELECT FROM CUSTOMER_ORDER WHERE ORDER_ID = {o.oRecordData['ORDER_ID']}) TO (SELECT FROM INVOICE WHERE ORDER_ID = {o.oRecordData['ORDER_ID']})"
                    )
                except:
                    # Zamówienie może nie mieć faktury, pomijamy
                    pass

            # User -> Invoice
            print("      -> User_to_invoice...")
            users_result = self.orient_client.command("SELECT FROM SYS_USER")
            for u in users_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE User_to_invoice FROM (SELECT FROM SYS_USER WHERE USER_ID = {u.oRecordData['USER_ID']}) TO (SELECT FROM INVOICE WHERE CREATED_BY = {u.oRecordData['USER_ID']})"
                    )
                except:
                    # User może nie mieć faktur, pomijamy
                    pass

            # Order -> Order_Item
            print("      -> Order_to_order_item...")
            orders_result = self.orient_client.command("SELECT FROM CUSTOMER_ORDER")
            for o in orders_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Order_to_order_item FROM (SELECT FROM CUSTOMER_ORDER WHERE ORDER_ID = {o.oRecordData['ORDER_ID']}) TO (SELECT FROM ORDER_ITEM WHERE ORDER_ID = {o.oRecordData['ORDER_ID']})"
                    )
                except:
                    # Zamówienie może nie mieć pozycji, pomijamy
                    pass

            # Product -> Order_Item
            print("      -> Product_to_order_item...")
            products_result = self.orient_client.command("SELECT FROM PRODUCT")
            for p in products_result:
                try:
                    self.orient_client.command(
                        f"CREATE EDGE Product_to_order_item FROM (SELECT FROM PRODUCT WHERE PRODUCT_ID = {p.oRecordData['PRODUCT_ID']}) TO (SELECT FROM ORDER_ITEM WHERE PRODUCT_ID = {p.oRecordData['PRODUCT_ID']})"
                    )
                except:
                    # Produkt może nie być w zamówieniach, pomijamy
                    pass

            print("✅ Replikacja do OrientDB zakończona sukcesem!")

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
