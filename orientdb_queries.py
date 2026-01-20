# orientdb_queries.py

"""
Ten plik przechowuje zapytania dla bazy OrientDB.
OrientDB wspiera zarówno SQL jak i zapytania grafowe (MATCH).
Wykorzystujemy krawędzie (edges) do wydajniejszych zapytań grafowych.
"""

# Limit wyników dla zapytań SELECT (ustawiony na -1 = bez limitu)
FETCH_LIMIT = -1

# ==========================================
# ZAPYTANIA ODCZYTUJACE (SELECT)
# ==========================================
ORIENT_SELECT_QUERIES = {
    "accountants": "SELECT NAME, EMAIL FROM SYS_USER WHERE ROLE = 'ACCOUNTANT'",

    "customers_by_city": "SELECT COUNT(*) AS NUMBER, CITY FROM CUSTOMER GROUP BY CITY",

    "payments_range": "SELECT PAYMENT_ID, INVOICE_ID, PAYMENT_DATE, AMOUNT, METHOD, CONFIRMED FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000",

    "products_low_price_stock": "SELECT SUM(STOCK_QUANTITY) AS SUMMARY FROM PRODUCT WHERE PRICE < 100",

    # JOIN z wykorzystaniem grafowych krawędzi (MATCH)
    "invoices_with_customers": """MATCH {class: CUSTOMER, as: c} -Customer_to_invoice-> {class: INVOICE, as: i}
                               RETURN i.CUSTOMER_ID as customer_id, i.INVOICE_NUMBER as invoice_number, i.TOTAL_AMOUNT as total_amount, c.NAME as customer_name""",

    # LEFT JOIN - używamy SQL z LET dla symulacji LEFT JOIN (MATCH nie wspiera LEFT JOIN)
    "invoices_with_payments": """SELECT i.INVOICE_ID as invoice_id, i.INVOICE_NUMBER as invoice_number,
                              p.METHOD as method, p.AMOUNT as amount
                              FROM INVOICE i
                              LET p = (SELECT FROM PAYMENT WHERE INVOICE_ID = i.INVOICE_ID)
                              ORDER BY i.INVOICE_ID""",

    # Raport: Ilość sztuk per kraj - używamy SQL dla poprawnej agregacji
    "report_quantity_per_country": """SELECT c.COUNTRY AS Kraj, p.NAME AS Nazwa_Produktu, SUM(oi.QUANTITY) AS Laczna_Ilosc_Sztuk
                                   FROM CUSTOMER c, CUSTOMER_ORDER co, ORDER_ITEM oi, PRODUCT p
                                   WHERE c.CUSTOMER_ID = co.CUSTOMER_ID
                                     AND co.ORDER_ID = oi.ORDER_ID
                                     AND oi.PRODUCT_ID = p.PRODUCT_ID
                                   GROUP BY c.COUNTRY, p.NAME
                                   ORDER BY c.COUNTRY ASC, Laczna_Ilosc_Sztuk DESC""",

    # Zlozony raport sprzedazowy - używamy SQL dla poprawnej agregacji z HAVING
    "complex_sales_report": """SELECT c.NAME AS Nazwa_Klienta, c.COUNTRY AS Kraj, u.USERNAME AS Agent,
                            COUNT(DISTINCT co.ORDER_ID) AS Liczba_Zrealizowanych_Zamowien,
                            COUNT(DISTINCT p.PRODUCT_ID) AS Liczba_Unikalnych_Produktow,
                            SUM(oi.QUANTITY) AS Laczna_Ilosc_Sztuk,
                            SUM(oi.QUANTITY * oi.UNIT_PRICE) AS Wartosc_Zamowien_Brutto,
                            MAX(i.ISSUE_DATE) AS Data_Ostatniej_Faktury
                            FROM CUSTOMER c, CUSTOMER_ORDER co, ORDER_ITEM oi, PRODUCT p, INVOICE i, SYS_USER u, PAYMENT pay
                            WHERE c.CUSTOMER_ID = co.CUSTOMER_ID
                              AND co.ORDER_ID = oi.ORDER_ID
                              AND oi.PRODUCT_ID = p.PRODUCT_ID
                              AND co.ORDER_ID = i.ORDER_ID
                              AND i.CREATED_BY = u.USER_ID
                              AND i.INVOICE_ID = pay.INVOICE_ID
                              AND co.STATUS = 'COMPLETED'
                              AND pay.CONFIRMED = 1
                            GROUP BY c.NAME, c.COUNTRY, u.USERNAME
                            HAVING SUM(oi.QUANTITY) > 10
                            ORDER BY Wartosc_Zamowien_Brutto DESC"""
}

# ==========================================
# ZAPYTANIA MODYFIKUJACE STRUKTURE (DDL)
# ==========================================
# Uwaga: OrientDB pozwala na dodawanie pól dynamicznie (schemaless),
# więc ALTER TABLE nie jest zawsze konieczny
ORIENT_DDL_QUERIES = {
    "add_column_past_due": "ALTER CLASS INVOICE CUSTOM PAST_DUE = BOOLEAN"
    # Alternatywnie można po prostu zacząć używać pola bez ALTER:
    # UPDATE INVOICE SET PAST_DUE = false WHERE PAST_DUE IS NULL
}

# ==========================================
# ZAPYTANIA AKTUALIZUJACE (UPDATE/DELETE)
# ==========================================
ORIENT_DML_QUERIES = {
    "mark_past_due": "UPDATE INVOICE SET PAST_DUE = 1 WHERE DUE_DATE < '2023-12-31'",

    # UPDATE z wykorzystaniem MATCH (grafowe zapytania)
    "reset_amount_for_apolonia": """UPDATE CUSTOMER_ORDER MERGE {'TOTAL_AMOUNT': 0} WHERE @rid IN (SELECT expand(o) FROM (MATCH {class: CUSTOMER, as: c, where: (NAME = 'Apolonia Banak')} -Customer_to_order-> {class: CUSTOMER_ORDER, as: o} RETURN o))""",

    "delete_specific_item": "DELETE VERTEX ORDER_ITEM WHERE PRODUCT_ID = 2",

    # Najpierw usuwamy powiazane ORDER_ITEM, potem PRODUCT
    "delete_order_items_for_products": "DELETE VERTEX ORDER_ITEM",

    "delete_all_products": "DELETE VERTEX PRODUCT"
}

# ==========================================
# POMOCNICZE FUNKCJE
# ==========================================

def execute_orient_query(client, query, fetch_plan="*:-1"):
    """
    Wykonuje zapytanie OrientDB i zwraca wyniki.

    Args:
        client: Klient pyorient
        query: String z zapytaniem
        fetch_plan: Plan pobierania (domyślnie "*:-1" = wszystko)

    Returns:
        Lista wyników
    """
    try:
        # Czyszczenie query ze zbędnych spacji
        clean_query = " ".join(query.split())
        result = client.query(clean_query, FETCH_LIMIT, fetch_plan)
        return result if result is not None else []
    except Exception as e:
        print(f"   [WARN] Blad execute_orient_query: {e}")
        raise e


def execute_orient_command(client, command):
    """
    Wykonuje komendę OrientDB (UPDATE, DELETE, ALTER).

    Args:
        client: Klient pyorient
        command: String z komendą

    Returns:
        Wynik komendy
    """
    try:
        # Czyszczenie command ze zbędnych spacji
        clean_command = " ".join(command.split())
        return client.command(clean_command)
    except Exception as e:
        print(f"   [WARN] Blad execute_orient_command: {e}")
        raise e
