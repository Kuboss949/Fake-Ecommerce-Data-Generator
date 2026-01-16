# cassandra_queries.py

"""
Ten plik przechowuje zapytania CQL dla bazy Cassandra.
Uwaga: W Cassandrze kluczowe jest podawanie Partition Key (np. role, city, year) w klauzuli WHERE.
Bez tego zapytania będą wymagały 'ALLOW FILTERING', co jest zabójcze dla wydajności.
"""

# ==========================================
# 🔍 ZAPYTANIA ODCZYTUJĄCE (SELECT)
# ==========================================
CQL_SELECT_QUERIES = {
    # SQL: SELECT ... FROM SYS_USER WHERE ROLE = 'ACCOUNTANT'
    # Wymaga podania roli (Partition Key)
    "accountants": """
                   SELECT *
                   FROM users_by_role
                   WHERE role = 'ACCOUNTANT'
                   """,

    # SQL: SELECT COUNT(*), CITY FROM CUSTOMER GROUP BY CITY
    # W Cassandrze zazwyczaj pytamy o konkretne miasto.
    # Agregacja dla WSZYSTKICH miast naraz robiona jest po stronie aplikacji lub Sparka.
    "customers_in_city": """
                         SELECT count(*)
                         FROM customers_by_city
                         WHERE city = '{city}'
                         """,

    # SQL: SELECT ... FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000
    # Musimy znać ROK (Partition Key), żeby posortować po kwocie (Clustering Key)
    "payments_range": """
                      SELECT *
                      FROM payments_by_year_amount
                      WHERE year = {year}
                        AND amount >= 10000
                        AND amount <= 40000
                      """,

    # SQL: SELECT SUM(STOCK_QUANTITY) FROM PRODUCT WHERE PRICE < 100
    # Używamy sztucznego bucketa 'all_products' jako Partition Key
    "products_low_price": """
                          SELECT name, stock_quantity, price
                          FROM products_by_price
                          WHERE bucket = 'all_products'
                            AND price < 100
                          """,

    # SQL: SELECT ... FROM INVOICE JOIN CUSTOMER ...
    # W Cassandrze dane są zdenormalizowane - wszystko jest w jednej tabeli
    "invoice_details": """
                       SELECT invoice_number, total_amount, customer_name, payment_method
                       FROM invoice_full_details
                       WHERE invoice_id = {invoice_id}
                       """,

    # Raport: Ilość sztuk per kraj (z tabeli SalesStatsByCountry)
    # Dane są już policzone przy wpisywaniu (write-time aggregation)
    "report_quantity_per_country": """
                                   SELECT product_name, total_quantity_sum
                                   FROM sales_stats_by_country_product
                                   WHERE country = '{country}'
                                   """,

    # Złożony raport sprzedażowy (Customer 360)
    # Dane są już policzone w tabeli CustomerLeaderboard
    "complex_sales_report": """
                            SELECT customer_name, agent_username, gross_value_brutto, orders_count
                            FROM customer_performance_leaderboard
                            WHERE country = '{country}'
                            """
}

# ==========================================
# 🛠️ ZAPYTANIA MODYFIKUJĄCE STRUKTURĘ (DDL)
# ==========================================
CQL_DDL_QUERIES = {
    "add_column_past_due": """
                           ALTER TABLE invoice_full_details
                               ADD past_due boolean
                           """
}

# ==========================================
# ✏️ ZAPYTANIA AKTUALIZUJĄCE I KASUJĄCE
# ==========================================
# W Cassandrze nie ma "UPDATE ... WHERE data < ..." dla całej tabeli.
# Musisz pobrać ID w aplikacji i aktualizować po ID.
CQL_DML_QUERIES = {
    # 1. UPDATE pojedynczej faktury (wymaga ID)
    "mark_past_due_single": """
                            UPDATE invoice_full_details
                            SET past_due = true
                            WHERE invoice_id = {invoice_id}
                            """,

    # 2. UPDATE po nazwisku (Apolonia Banak) - KROK 1: Znajdź ID
    "find_invoice_ids_by_name": """
                                SELECT invoice_id, total_amount
                                FROM invoices_by_customer_name
                                WHERE customer_name = '{customer_name}'
                                """,

    # 2. UPDATE po nazwisku - KROK 2: Zaktualizuj konkretną fakturę (w tabeli głównej)
    "update_invoice_amount": """
                             UPDATE invoice_full_details
                             SET total_amount = 0
                             WHERE invoice_id = {invoice_id}
                             """,
    # Uwaga: Należy też zaktualizować tabelę pomocniczą invoices_by_customer_name,
    # żeby zachować spójność! Cassandra nie robi tego sama.

    # 3. DELETE po produkcie - KROK 1: Znajdź, gdzie ten produkt występuje
    "find_orders_with_product": """
                                SELECT invoice_id
                                FROM order_items_by_product
                                WHERE product_id = {product_id}
                                """,

    # 3. DELETE po produkcie - KROK 2: Usuń wpis z indeksu
    "delete_product_lookup": """
                             DELETE
                             FROM order_items_by_product
                             WHERE product_id = {product_id}
                               AND invoice_id = {invoice_id}
                             """,

    # 4. DELETE ALL PRODUCTS
    "delete_all_products": """
        TRUNCATE products_by_price
    """
}