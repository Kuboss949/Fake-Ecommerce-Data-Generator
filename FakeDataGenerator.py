import unicodedata
import csv
import random
from collections import defaultdict  # Do agregacji danych w pamięci
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy.orm import Session
from sqlalchemy import select

# Import Twoich modeli SQL
from modele import Base, SysUser, Customer, Product, CustomerOrder, OrderItem, Invoice, Payment

#import i inicjalizowanie połączenia z orient 1.5.6
import pyorient       
orient_engine = pyorient.OrientDB("localhost", 2424)
orient_engine.set_session_token(True)
orient_session = orient_engine.connect("root", "root")
orient_engine.db_open( 'company', "root", "root" )

def create_tables_orient():
    orient_engine.command("create class  CUSTOMER extends V")
    orient_engine.command("create class  CUSTOMER_ORDER extends V")
    orient_engine.command("create class  INVOICE extends V")
    orient_engine.command("create class  ORDER_ITEM extends V")
    orient_engine.command("create class  PAYMENT extends V")
    orient_engine.command("create class  PRODUCT extends V")
    orient_engine.command("create class SYS_USER extends V")
    

# Import modeli Cassandry (zakładam, że plik to cassandra_tables.py)
from cassandra_tables import (
    UsersByRole, CustomersByCity, ProductsByPrice,
    InvoiceFullDetails, PaymentsByYearAmount,
    SalesStatsByCountry, CustomerLeaderboard,
    init_cassandra_schema
)


class FakeDataGenerator:
    def __init__(self, session: Session):
        self.fake = Faker('pl_PL')
        self.session = session

        # --- CASSANDRA INIT ---
        # Inicjalizujemy połączenie i tabele przy starcie
        init_cassandra_schema()

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
        # Symulujemy ID faktury (w realu pobralibysmy po save)
        fake_invoice_id = random.randint(1, 10000000)

        InvoiceFullDetails.create(
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

    def run_generation(self, num_users: int, num_customers: int, num_orders: int):
        print("Przed start: Generowanie tabel orientDB")
        create_tables_orient()

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
            print("Commit SQL...")
            self.session.commit()
            print("Gotowe.")
        except Exception as e:
            print(f"BŁĄD SQL: {e}")
            self.session.rollback()
        finally:
            self.session.close()


        print("Usuwanie starych danych z OrientDB")
        orient_engine.command("SELECT drop_all_data()")
        #upload to orientDB
        print("Dodawanie danych do OrientDB")
        #upload to orientDB
        orient_customers = self.session.execute(select(Customer))
        orient_customers_scalar = orient_customers.scalars().all()
        for c in orient_customers_scalar:
            customer_make = "insert into CUSTOMER set CUSTOMER_ID =  %d, NAME =  '%s', EMAIL = '%s' ,PHONE = '%s', ADDRESS = '%s', CITY = '%s', COUNTRY = '%s'"\
            % (c.CUSTOMER_ID, c.NAME, c.EMAIL, c.PHONE, c.ADDRESS, c.CITY, c.COUNTRY)
            orient_engine.command(customer_make)

        orient_users = self.session.execute(select(SysUser))
        orient_users_scalar = orient_users.scalars().all()
        for u in orient_users_scalar:
            user_make = "insert into SYS_USER set USER_ID =  %d, USERNAME = '%s' ,PASSWORD_HASH = '%s', NAME = '%s', SURNAME = '%s', EMAIL = '%s', ROLE = '%s', ACTIVE = '%s'"\
            % (u.USER_ID, u.USERNAME, u.PASSWORD_HASH, u.NAME, u.SURNAME, u.EMAIL, u.ROLE, u.ACTIVE)
            orient_engine.command(user_make)

        orient_customer_orders = self.session.execute(select(CustomerOrder))
        orient_customer_orders_scalar = orient_customer_orders.scalars().all()
        for c in orient_customer_orders_scalar:
            orders_make = "insert into CUSTOMER_ORDER set ORDER_ID =  %d, ORDER_ID =  %d, CUSTOMER_ID = %d ,ORDER_DATE = '%s', STATUS = '%s', TOTAL_AMOUNT = %f" \
            % (c.ORDER_ID, c.ORDER_ID, c.CUSTOMER_ID, c.ORDER_DATE, c.STATUS, c.TOTAL_AMOUNT)
            orient_engine.command(orders_make)
 
        orient_invoice = self.session.execute(select(Invoice))
        orient_invoice_scalar = orient_invoice.scalars().all()
        for i in orient_invoice_scalar:
            invoice_make = "insert into INVOICE set INVOICE_ID = %d, INVOICE_NUMBER =  '%s', CUSTOMER_ID = '%s', ORDER_ID = '%s', STATUS = '%s',  ISSUE_DATE = '%s', DUE_DATE = '%s', TOTAL_AMOUNT = '%s', CREATED_BY = '%s'" \
            % (i.INVOICE_ID, i.INVOICE_NUMBER, i.CUSTOMER_ID, i.ORDER_ID, i.STATUS, i.ISSUE_DATE, i.DUE_DATE, i.TOTAL_AMOUNT, i.CREATED_BY)
            orient_engine.command(invoice_make)
            
        orient_payment = self.session.execute(select(Payment))
        orient_payment_scalar = orient_payment.scalars().all()
        for p in orient_payment_scalar:
            payment_make = "insert into PAYMENT set PAYMENT_ID =  %d , INVOICE_ID =  %d ,PAYMENT_DATE = '%s', AMOUNT = '%s', METHOD = '%s', CONFIRMED = '%s'" \
            % (p.PAYMENT_ID, p.INVOICE_ID, p.PAYMENT_DATE, p.AMOUNT, p.METHOD, p.CONFIRMED)
            orient_engine.command(payment_make)

        orient_products = self.session.execute(select(Product))
        orient_products_scalar = orient_products.scalars().all()
        for p in orient_products_scalar:
            product_make = "insert into PRODUCT set PRODUCT_ID =  %d, NAME = '%s' ,DESCRIPTION = '%s', PRICE = %06.2f, STOCK_QUANTITY = %d" \
            % (p.PRODUCT_ID, p.NAME, p.DESCRIPTION, p.PRICE, p.STOCK_QUANTITY)
            orient_engine.command(product_make)

        orient_order_items = self.session.execute(select(OrderItem))
        orient_order_items_scalar = orient_order_items.scalars().all()
        for o in orient_order_items_scalar:
            order_item_make = "insert into ORDER_ITEM set ORDER_ITEM_ID = %d, ORDER_ID = %d, PRODUCT_ID = %d, QUANTITY = %d, UNIT_PRICE = %f" \
            % (o.ORDER_ITEM_ID, o.ORDER_ID, o.PRODUCT_ID, o.QUANTITY, o.UNIT_PRICE)
            orient_engine.command(order_item_make)
            
        print("Zakończono dodawanie danych do OrientDB")

        print("Dodawanie połączeń w OrientDB")
        orient_engine.command("SELECT fill_edge()")
        print("Zakończono dodawania połączeń do OrientDB")
        orient_engine.db_close()