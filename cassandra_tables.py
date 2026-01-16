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
# 2. Grupowanie (GROUP BY CITY)
# SQL: SELECT COUNT(*), CITY FROM CUSTOMER GROUP BY CITY
# ==========================================
class CustomersByCity(Model):
    __table_name__ = 'customers_by_city'

    # Partition Key
    city = columns.Text(partition_key=True)

    # Clustering Key
    customer_id = columns.Integer(primary_key=True)

    name = columns.Text()
    email = columns.Text()


# ==========================================
# 3. Zakresy liczbowe - Płatności
# SQL: SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000
# ==========================================
class PaymentsByYearAmount(Model):
    __table_name__ = 'payments_by_year_amount'

    # Partition Key
    year = columns.Integer(partition_key=True)

    # Clustering Key
    amount = columns.Decimal(primary_key=True, clustering_order="ASC")
    payment_id = columns.Integer(primary_key=True)

    # Dane
    method = columns.Text()
    payment_date = columns.DateTime()
    confirmed = columns.Boolean()


# ==========================================
# SQL: SELECT SUM(STOCK_QUANTITY) FROM PRODUCT WHERE PRICE < 100
# ==========================================
class ProductsByPrice(Model):
    __table_name__ = 'products_by_price'
    bucket = columns.Text(partition_key=True, default="all_products")
    price = columns.Decimal(primary_key=True, clustering_order="ASC")
    product_id = columns.Integer(primary_key=True)
    name = columns.Text()
    stock_quantity = columns.Integer()


# ==========================================
# SQL: JOINY oraz UPDATE/ALTER
# ==========================================
class InvoiceFullDetails(Model):
    __table_name__ = 'invoice_full_details'

    invoice_id = columns.Integer(partition_key=True)

    invoice_number = columns.Text()
    issue_date = columns.Date()
    due_date = columns.Date()
    total_amount = columns.Decimal()
    status = columns.Text()

    past_due = columns.Boolean(default=False)

    customer_id = columns.Integer()
    customer_name = columns.Text()
    customer_email = columns.Text()


    payment_method = columns.Text()
    payment_amount = columns.Decimal()
    payment_confirmed = columns.Boolean()


# ==========================================
# Agregacja: Sprzedaż wg Kraju i Produktu
# ==========================================
class SalesStatsByCountry(Model):
    __table_name__ = 'sales_stats_by_country_product'

    # Partition Key
    country = columns.Text(partition_key=True)

    # Clustering Keys
    total_quantity_sum = columns.Integer(primary_key=True, clustering_order="DESC")
    product_name = columns.Text(primary_key=True, clustering_order="ASC")

    # Można dodać product_id dla referencji
    product_id = columns.Integer()


# ==========================================
# Agregacja: Customer 360
# ==========================================
class CustomerLeaderboard(Model):
    __table_name__ = 'customer_performance_leaderboard'

    country = columns.Text(partition_key=True)

    gross_value_brutto = columns.Decimal(primary_key=True, clustering_order="DESC")

    customer_name = columns.Text(primary_key=True, clustering_order="ASC")

    agent_username = columns.Text()
    orders_count = columns.Integer()
    unique_products_count = columns.Integer()
    total_items_quantity = columns.Integer()
    last_invoice_date = columns.Date()


# ==========================================
# Funkcja usuwająca strukturę bazy
# ==========================================
def drop_cassandra_schema(keyspace='my_keyspace', nodes=['127.0.0.1']):
    print(f"Łączenie z klastrem Cassandra: {nodes}...")
    connection.setup(nodes, 'system', protocol_version=4)

    print(f"Usuwanie Keyspace '{keyspace}'...")
    from cassandra.cluster import Cluster
    cluster = Cluster(nodes)
    session = cluster.connect()
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        print(f"Keyspace '{keyspace}' został usunięty.")
    except Exception as e:
        print(f"Błąd podczas usuwania keyspace: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()


# ==========================================
# Funkcja inicjalizująca bazę
# ==========================================
def init_cassandra_schema(keyspace='my_keyspace', nodes=['127.0.0.1']):
    print(f"1. Łączenie z klastrem Cassandra: {nodes}...")

    connection.setup(nodes, 'system', protocol_version=4)

    print(f"2. Tworzenie Keyspace '{keyspace}' (jeśli nie istnieje)...")
    create_keyspace_simple(keyspace, replication_factor=1)

    print(f"3. Ustawianie domyślnego Keyspace na '{keyspace}'...")
    connection.setup(nodes, keyspace, protocol_version=4)

    print("4. Synchronizacja tabel (tworzenie struktur)...")
    sync_table(UsersByRole)
    sync_table(CustomersByCity)
    sync_table(PaymentsByYearAmount)
    sync_table(ProductsByPrice)
    sync_table(InvoiceFullDetails)
    sync_table(SalesStatsByCountry)
    sync_table(CustomerLeaderboard)
    sync_table(InvoicesByCustomerName)
    sync_table(OrderItemsByProduct)

    print("Gotowe! Struktura Cassandry została zainicjowana.")



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