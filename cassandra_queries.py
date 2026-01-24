# cassandra_queries.py

"""
Ten plik przechowuje zapytania CQL dla bazy Cassandra.
NOWA STRUKTURA - zapytania dopasowane do tabel z bucketami pozwalajacymi
pobierac wszystkie dane bez znajomosci partition key.
"""

# ==========================================
# ZAPYTANIA ODCZYTUJACE (SELECT)
# ==========================================
CQL_SELECT_QUERIES = {
    # SQL: SELECT ... FROM SYS_USER WHERE ROLE = 'ACCOUNTANT'
    # Wymaga podania roli (Partition Key) - bez zmian
    "accountants": """
                   SELECT *
                   FROM users_by_role
                   WHERE role = 'ACCOUNTANT'
                   """,

    # SQL: SELECT COUNT(*), CITY FROM CUSTOMER GROUP BY CITY
    # NOWE: Pobiera wszystkie miasta z gotowymi licznikami
    "customers_by_city": """
                         SELECT city, customer_count
                         FROM customer_count_by_city
                         WHERE bucket = 'all'
                         """,

    # SQL: SELECT ... FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000
    # NOWE: Pobiera wszystkie platnosci z zakresu 10k-40k (niezaleznie od roku)
    "payments_range": """
                      SELECT payment_id, invoice_id, payment_date, amount, method, confirmed
                      FROM payments_by_amount_range
                      WHERE amount_bucket = '10k-40k'
                      """,

    # SQL: SELECT SUM(STOCK_QUANTITY) FROM PRODUCT WHERE PRICE < 100
    # NOWE: Pobiera gotowa sume z tabeli agregatow
    "products_low_price_stock": """
                          SELECT total_stock
                          FROM product_stock_aggregates
                          WHERE price_bucket = 'under_100'
                          """,

    # SQL: SELECT ... FROM INVOICE JOIN CUSTOMER ...
    # NOWE: Pobiera wszystkie faktury (partycjonowane po roku)
    # Rozszerzony zakres lat aby objac wszystkie dane
    "invoices_with_customers": """
                       SELECT invoice_number, total_amount, customer_name
                       FROM all_invoices_with_details
                       WHERE year IN (2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030)
                       """,

    # SQL: SELECT ... FROM INVOICE LEFT JOIN PAYMENT ...
    # NOWE: Pobiera wszystkie faktury z danymi platnosci
    "invoices_with_payments": """
                       SELECT invoice_id, invoice_number, payment_method, payment_amount
                       FROM all_invoices_with_details
                       WHERE year IN (2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030)
                       """,

    # SQL: Raport ilosc sztuk per kraj
    # NOWE: bucket='all' pozwala pobrac wszystkie kraje
    "report_quantity_per_country": """
                                   SELECT country, product_name, total_quantity_sum
                                   FROM sales_stats_all
                                   WHERE bucket = 'all'
                                   """,

    # SQL: Zlozony raport sprzedazowy (Customer 360)
    # NOWE: bucket='all' pozwala pobrac wszystkich klientow
    "complex_sales_report": """
                            SELECT customer_name, country, agent_username, orders_count,
                                   unique_products_count, total_items_quantity,
                                   gross_value_brutto, last_invoice_date
                            FROM customer_leaderboard_all
                            WHERE bucket = 'all'
                            """
}

# ==========================================
# ZAPYTANIA MODYFIKUJACE STRUKTURE (DDL)
# ==========================================
CQL_DDL_QUERIES = {
    # Cassandra nie wspiera IF NOT EXISTS dla ADD, wiec ignorujemy blad jesli kolumna istnieje
    "add_column_past_due": """
                           ALTER TABLE all_invoices_with_details
                               ADD past_due boolean
                           """
}

# ==========================================
# ZAPYTANIA AKTUALIZUJACE I KASUJACE
# ==========================================
# W Cassandrze nie ma "UPDATE ... WHERE data < ..." dla calej tabeli.
# Musisz pobrac ID w aplikacji i aktualizowac po ID.
CQL_DML_QUERIES = {
    # 1. UPDATE pojedynczej faktury (wymaga year + invoice_id)
    "mark_past_due_single": """
                            UPDATE all_invoices_with_details
                            SET past_due = true
                            WHERE year = {year} AND invoice_id = {invoice_id}
                            """,

    # 2. UPDATE po nazwisku (Apolonia Banak) - KROK 1: Znajdz ID
    "find_invoice_ids_by_name": """
                                SELECT invoice_id, total_amount
                                FROM invoices_by_customer_name
                                WHERE customer_name = '{customer_name}'
                                """,

    # 2. UPDATE po nazwisku - KROK 2: Zaktualizuj konkretna fakture
    "update_invoice_amount": """
                             UPDATE all_invoices_with_details
                             SET total_amount = 0
                             WHERE year = {year} AND invoice_id = {invoice_id}
                             """,

    # 3. DELETE po produkcie - KROK 1: Znajdz, gdzie ten produkt wystepuje
    "find_orders_with_product": """
                                SELECT invoice_id
                                FROM order_items_by_product
                                WHERE product_id = {product_id}
                                """,

    # 3. DELETE po produkcie - KROK 2: Usun wpis z indeksu
    "delete_product_lookup": """
                             DELETE
                             FROM order_items_by_product
                             WHERE product_id = {product_id}
                               AND invoice_id = {invoice_id}
                             """,

    # 4. Najpierw usuwamy powiazane ORDER_ITEM (Cassandra: TRUNCATE tabeli)
    "delete_order_items_for_products": """
        TRUNCATE order_items_by_product
    """,

    # 5. DELETE ALL PRODUCTS - UWAGA: nie mozna truncate tabeli z counterami
    # Trzeba uzyc DELETE dla kazdego wiersza lub drop/recreate
    "delete_all_products": """
        TRUNCATE order_items_by_product
    """
}
