from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra import ConsistencyLevel


# ==========================================
# 1. Proste filtrowanie (WHERE ROLE = ...)
# SQL: SELECT NAME, EMAIL FROM SYS_USER WHERE ROLE = 'ACCOUNTANT'
# ==========================================
class UsersByRole(Model):
    __table_name__ = 'users_by_role'

    # Partition Key
    role = columns.Text(partition_key=True)

    # Clustering Key
    user_id = columns.Integer(primary_key=True)

    username = columns.Text()
    name = columns.Text()
    email = columns.Text()


# ==========================================
# 2. Grupowanie (GROUP BY CITY) - NOWA STRUKTURA
# SQL: SELECT COUNT(*), CITY FROM CUSTOMER GROUP BY CITY
# Rozwiazanie: Counter table z bucketem 'all'
# ==========================================
class CustomerCountByCity(Model):
    __table_name__ = 'customer_count_by_city'

    # Sztuczny bucket - pozwala pobrac wszystko jednym zapytaniem
    bucket = columns.Text(partition_key=True)

    # Clustering key
    city = columns.Text(primary_key=True)

    # Licznik - aktualizowany przy kazdym INSERCIE klienta
    customer_count = columns.Counter()


# ==========================================
# 3. Zakresy liczbowe - Platnosci - NOWA STRUKTURA
# SQL: SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000
# Rozwiazanie: Partition key = amount_bucket
# ==========================================
class PaymentsByAmountRange(Model):
    __table_name__ = 'payments_by_amount_range'

    # Bucket dla zakresu kwot (np. "10k-40k")
    amount_bucket = columns.Text(partition_key=True)

    # Clustering keys
    amount = columns.Decimal(primary_key=True, clustering_order="ASC")
    payment_id = columns.Integer(primary_key=True)

    # Dane
    invoice_id = columns.Integer()
    method = columns.Text()
    payment_date = columns.DateTime()
    confirmed = columns.Boolean()


# ==========================================
# 4. Agregacja SUM(STOCK_QUANTITY) - NOWA STRUKTURA
# SQL: SELECT SUM(STOCK_QUANTITY) FROM PRODUCT WHERE PRICE < 100
# Rozwiazanie: Counter table z gotowa suma
# ==========================================
class ProductStockAggregates(Model):
    __table_name__ = 'product_stock_aggregates'

    # Bucket dla zakresu cen
    price_bucket = columns.Text(partition_key=True)  # "under_100", "100_500", "over_500"

    # Dummy clustering key (wymagany dla counter)
    dummy = columns.Integer(primary_key=True, default=1)

    # Gotowa suma - aktualizowana przy kazdym INSERCIE produktu
    total_stock = columns.Counter()


# ==========================================
# 5. Wszystkie faktury z detalami - NOWA STRUKTURA
# SQL: JOINY INVOICE + CUSTOMER + PAYMENT
# Rozwiazanie: Partycjonowanie po roku
# ==========================================
class AllInvoicesWithDetails(Model):
    __table_name__ = 'all_invoices_with_details'

    # Partition key - rok
    year = columns.Integer(partition_key=True)

    # Clustering keys
    invoice_id = columns.Integer(primary_key=True)

    # Dane z INVOICE
    invoice_number = columns.Text()
    total_amount = columns.Decimal()
    status = columns.Text()
    issue_date = columns.Date()
    due_date = columns.Date()
    past_due = columns.Boolean(default=False)

    # Dane z CUSTOMER (zdenormalizowane)
    customer_id = columns.Integer()
    customer_name = columns.Text()
    customer_email = columns.Text()

    # Dane z PAYMENT (zdenormalizowane)
    payment_method = columns.Text()
    payment_amount = columns.Decimal()
    payment_confirmed = columns.Boolean()


# ==========================================
# 6. Raport sprzedazy per kraj/produkt - NOWA STRUKTURA
# SQL: SELECT country, product, SUM(qty) GROUP BY country, product
# Rozwiazanie: bucket='all' pozwala pobrac wszystko
# ==========================================
class SalesStatsByCountryAll(Model):
    __table_name__ = 'sales_stats_all'

    # Bucket pozwalajacy pobrac wszystko
    bucket = columns.Text(partition_key=True)

    # Clustering keys
    country = columns.Text(primary_key=True)
    product_name = columns.Text(primary_key=True)

    # Dane
    total_quantity_sum = columns.Integer()
    product_id = columns.Integer()


# ==========================================
# 7. Customer Leaderboard - NOWA STRUKTURA
# SQL: Complex sales report
# Rozwiazanie: bucket='all' pozwala pobrac wszystko
# ==========================================
class CustomerLeaderboardAll(Model):
    __table_name__ = 'customer_leaderboard_all'

    # Bucket pozwalajacy pobrac wszystko
    bucket = columns.Text(partition_key=True)

    # Clustering keys - sortowanie po wartosci brutto DESC
    gross_value_brutto = columns.Decimal(primary_key=True, clustering_order="DESC")
    customer_name = columns.Text(primary_key=True)

    # Dane
    country = columns.Text()
    agent_username = columns.Text()
    orders_count = columns.Integer()
    unique_products_count = columns.Integer()
    total_items_quantity = columns.Integer()
    last_invoice_date = columns.Date()


# ==========================================
# Tabele pomocnicze dla UPDATE/DELETE
# ==========================================

# Tabela pomocnicza dla UPDATE po nazwie klienta
class InvoicesByCustomerName(Model):
    __table_name__ = 'invoices_by_customer_name'
    customer_name = columns.Text(partition_key=True)
    invoice_id = columns.Integer(primary_key=True)
    total_amount = columns.Decimal()
    status = columns.Text()


# Tabela pomocnicza dla DELETE po ID produktu
class OrderItemsByProduct(Model):
    __table_name__ = 'order_items_by_product'
    product_id = columns.Integer(partition_key=True)
    invoice_id = columns.Integer(primary_key=True)
    quantity = columns.Integer()
    unit_price = columns.Decimal()


# ==========================================
# Funkcja usuwajaca strukture bazy
# ==========================================
def drop_cassandra_schema(keyspace='my_keyspace', nodes=['127.0.0.1']):
    print(f"Laczenie z klastrem Cassandra: {nodes}...")
    connection.setup(nodes, 'system', protocol_version=4)

    print(f"Usuwanie Keyspace '{keyspace}'...")
    from cassandra.cluster import Cluster
    cluster = Cluster(nodes)
    session = cluster.connect()
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        print(f"Keyspace '{keyspace}' zostal usuniety.")
    except Exception as e:
        print(f"Blad podczas usuwania keyspace: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()


# ==========================================
# Funkcja inicjalizujaca baze
# ==========================================
def init_cassandra_schema(keyspace='my_keyspace', nodes=['127.0.0.1']):
    print(f"1. Laczenie z klastrem Cassandra: {nodes}...")

    connection.setup(nodes, 'system', protocol_version=4)

    print(f"2. Tworzenie Keyspace '{keyspace}' (jesli nie istnieje)...")
    create_keyspace_simple(keyspace, replication_factor=1)

    print(f"3. Ustawianie domyslnego Keyspace na '{keyspace}'...")
    connection.setup(nodes, keyspace, protocol_version=4)

    print("4. Synchronizacja tabel (tworzenie struktur)...")
    sync_table(UsersByRole)
    sync_table(CustomerCountByCity)
    sync_table(PaymentsByAmountRange)
    sync_table(ProductStockAggregates)
    sync_table(AllInvoicesWithDetails)
    sync_table(SalesStatsByCountryAll)
    sync_table(CustomerLeaderboardAll)
    sync_table(InvoicesByCustomerName)
    sync_table(OrderItemsByProduct)

    print("Gotowe! Struktura Cassandry zostala zainicjowana.")
