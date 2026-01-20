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

    # LEFT JOIN - OrientDB: pobieramy wszystkie faktury, payment moze byc null
    # Uzywamy SELECT z LET przed FROM (skladnia OrientDB)
    "invoices_with_payments": """SELECT INVOICE_ID as invoice_id, INVOICE_NUMBER as invoice_number,
                              $p.METHOD as method, $p.AMOUNT as amount
                              FROM INVOICE
                              LET $p = (SELECT FROM PAYMENT WHERE INVOICE_ID = $parent.current.INVOICE_ID)
                              ORDER BY INVOICE_ID""",

    # Raport: Ilość sztuk per kraj - MATCH z agregacja
    "report_quantity_per_country": """MATCH {class: CUSTOMER, as: c} -Customer_to_order-> {class: CUSTOMER_ORDER, as: co} -Order_to_order_item-> {class: ORDER_ITEM, as: oi} <-Product_to_order_item- {class: PRODUCT, as: p}
                                   RETURN c.COUNTRY as Kraj, p.NAME as Nazwa_Produktu, sum(oi.QUANTITY) as Laczna_Ilosc_Sztuk
                                   GROUP BY Kraj, Nazwa_Produktu
                                   ORDER BY Kraj ASC, Laczna_Ilosc_Sztuk DESC""",

    # Zlozony raport sprzedazowy - MATCH z filtrami i agregacja
    "complex_sales_report": """MATCH
                            {class: CUSTOMER, as: c} -Customer_to_order-> {class: CUSTOMER_ORDER, as: co, where: (STATUS = 'COMPLETED')} -Order_to_order_item-> {class: ORDER_ITEM, as: oi} <-Product_to_order_item- {class: PRODUCT, as: p},
                            {as: co} -Order_to_invoice-> {class: INVOICE, as: i} -Invoice_to_payment-> {class: PAYMENT, as: pay, where: (CONFIRMED = 1)},
                            {as: i} <-User_to_invoice- {class: SYS_USER, as: u}
                            RETURN c.NAME as Nazwa_Klienta, c.COUNTRY as Kraj, u.USERNAME as Agent,
                                   count(DISTINCT(co.ORDER_ID)) as Liczba_Zrealizowanych_Zamowien,
                                   count(DISTINCT(p.PRODUCT_ID)) as Liczba_Unikalnych_Produktow,
                                   sum(oi.QUANTITY) as Laczna_Ilosc_Sztuk,
                                   sum(oi.QUANTITY * oi.UNIT_PRICE) as Wartosc_Zamowien_Brutto,
                                   max(i.ISSUE_DATE) as Data_Ostatniej_Faktury
                            GROUP BY Nazwa_Klienta, Kraj, Agent
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
