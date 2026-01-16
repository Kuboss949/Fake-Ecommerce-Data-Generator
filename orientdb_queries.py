# orientdb_queries.py

"""
Ten plik przechowuje zapytania dla bazy OrientDB.
OrientDB wspiera zarówno SQL jak i zapytania grafowe (MATCH).
Wykorzystujemy krawędzie (edges) do wydajniejszych zapytań grafowych.
"""

# Limit wyników dla zapytań SELECT (można nadpisać w benchmarku)
FETCH_LIMIT = 100

# ==========================================
# 🔍 ZAPYTANIA ODCZYTUJĄCE (SELECT)
# ==========================================
ORIENT_SELECT_QUERIES = {
    "accountants": """
                   SELECT NAME, EMAIL
                   FROM SYS_USER
                   WHERE ROLE = 'ACCOUNTANT'
                   """,

    "customers_by_city": """
                         SELECT COUNT(*) AS NUMBER, CITY
                         FROM CUSTOMER
                         GROUP BY CITY
                         """,

    "payments_range": """
                      SELECT *
                      FROM PAYMENT
                      WHERE AMOUNT BETWEEN 10000 AND 40000
                      """,

    "products_low_price_stock": """
                                SELECT SUM(STOCK_QUANTITY) AS SUMMARY
                                FROM PRODUCT
                                WHERE PRICE < 100
                                """,

    # JOIN z wykorzystaniem grafowych krawędzi (MATCH)
    "invoices_with_customers": """
                               MATCH {class: CUSTOMER, as: c} -Customer_to_invoice-> {class: INVOICE, as: i}
                               RETURN i.CUSTOMER_ID as customer_id,
                                      i.INVOICE_NUMBER as invoice_number,
                                      i.TOTAL_AMOUNT as total_amount,
                                      c.NAME as customer_name
                               """,

    # LEFT JOIN z wykorzystaniem grafowych krawędzi
    "invoices_with_payments": """
                              MATCH {class: INVOICE, as: i} -Invoice_to_payment-> {class: PAYMENT, as: p}
                              RETURN i.INVOICE_ID as invoice_id,
                                     i.INVOICE_NUMBER as invoice_number,
                                     p.METHOD as method,
                                     p.AMOUNT as amount
                              ORDER BY i.INVOICE_ID
                              """,

    # Raport: Ilość sztuk per kraj - wykorzystanie grafowych relacji
    "report_quantity_per_country": """
                                   MATCH {class: CUSTOMER, as: c}
                                         -Customer_to_order-> {class: CUSTOMER_ORDER, as: co}
                                         -Order_to_order_item-> {class: ORDER_ITEM, as: oi}
                                         <-Product_to_order_item- {class: PRODUCT, as: p}
                                   RETURN c.COUNTRY as Kraj,
                                          p.NAME as Nazwa_Produktu,
                                          SUM(oi.QUANTITY) AS Laczna_Ilosc_Sztuk
                                   GROUP BY c.COUNTRY, p.NAME
                                   ORDER BY c.COUNTRY ASC, Laczna_Ilosc_Sztuk DESC
                                   """,

    # Złożony raport sprzedażowy - pełna moc grafowych zapytań
    "complex_sales_report": """
                            MATCH
                                {class: CUSTOMER, as: c}
                                    -Customer_to_order-> {class: CUSTOMER_ORDER, as: co, where: (STATUS = 'COMPLETED')}
                                    -Order_to_order_item-> {class: ORDER_ITEM, as: oi}
                                    <-Product_to_order_item- {class: PRODUCT, as: p},
                                {as: co}
                                    -Order_to_invoice-> {class: INVOICE, as: i}
                                    -Invoice_to_payment-> {class: PAYMENT, as: pay, where: (CONFIRMED = 1)},
                                {as: i}
                                    <-User_to_invoice- {class: SYS_USER, as: u}
                            RETURN
                                c.NAME AS Nazwa_Klienta,
                                c.COUNTRY AS Kraj,
                                u.USERNAME AS Agent,
                                COUNT(DISTINCT(co.ORDER_ID)) AS Liczba_Zrealizowanych_Zamowien,
                                COUNT(DISTINCT(p.PRODUCT_ID)) AS Liczba_Unikalnych_Produktow,
                                SUM(oi.QUANTITY) AS Laczna_Ilosc_Sztuk,
                                SUM(oi.QUANTITY * oi.UNIT_PRICE) AS Wartosc_Zamowien_Brutto,
                                MAX(i.ISSUE_DATE) AS Data_Ostatniej_Faktury
                            GROUP BY c.NAME, c.COUNTRY, u.USERNAME
                            HAVING SUM(oi.QUANTITY) > 10
                            ORDER BY Wartosc_Zamowien_Brutto DESC
                            """
}

# ==========================================
# 🛠️ ZAPYTANIA MODYFIKUJĄCE STRUKTURĘ (DDL)
# ==========================================
# Uwaga: OrientDB pozwala na dodawanie pól dynamicznie (schemaless),
# więc ALTER TABLE nie jest zawsze konieczny
ORIENT_DDL_QUERIES = {
    "add_column_past_due": """
                           ALTER CLASS INVOICE CUSTOM PAST_DUE = BOOLEAN
                           """
    # Alternatywnie można po prostu zacząć używać pola bez ALTER:
    # UPDATE INVOICE SET PAST_DUE = false WHERE PAST_DUE IS NULL
}

# ==========================================
# ✏️ ZAPYTANIA AKTUALIZUJĄCE (UPDATE/DELETE)
# ==========================================
ORIENT_DML_QUERIES = {
    "mark_past_due": """
                     UPDATE INVOICE
                     SET PAST_DUE = 1
                     WHERE DUE_DATE < '2023-12-31'
                     """,

    # UPDATE z wykorzystaniem MATCH (grafowe zapytania)
    "reset_amount_for_apolonia": """
                                 UPDATE CUSTOMER_ORDER
                                 MERGE {'TOTAL_AMOUNT': 0}
                                 WHERE @rid IN (
                                     SELECT expand(o)
                                     FROM (
                                         MATCH {class: CUSTOMER, as: c, where: (NAME = 'Apolonia Banak')}
                                               -Customer_to_order-> {class: CUSTOMER_ORDER, as: o}
                                         RETURN o
                                     )
                                 )
                                 """,

    "delete_specific_item": """
                            DELETE VERTEX
                            FROM ORDER_ITEM
                            WHERE PRODUCT_ID = 2
                            """,

    "delete_all_products": """
                           DELETE VERTEX
                           FROM PRODUCT
                           """
}

# ==========================================
# 📊 POMOCNICZE FUNKCJE
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
    return client.query(query, FETCH_LIMIT, fetch_plan)


def execute_orient_command(client, command):
    """
    Wykonuje komendę OrientDB (UPDATE, DELETE, ALTER).

    Args:
        client: Klient pyorient
        command: String z komendą

    Returns:
        Wynik komendy
    """
    return client.command(command)
