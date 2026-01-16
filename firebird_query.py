# sql_queries.py

"""
Ten plik przechowuje zapytania SQL do wykorzystania w generatorze danych.
Placeholder'y w nawiasach klamrowych {} są przeznaczone do użycia z metodą .format() w Pythonie.
"""

# ==========================================
# ZAPYTANIA ODCZYTUJACE (SELECT)
# ==========================================
SELECT_QUERIES = {
    "accountants": """
                   SELECT NAME, EMAIL
                   FROM SYS_USER
                   WHERE ROLE = 'ACCOUNTANT'
                   """,

    "customers_by_city": """
                         SELECT COUNT(*) as Liczba, CITY
                         FROM CUSTOMER
                         GROUP BY CITY
                         """,

    "payments_range": """
                      SELECT *
                      FROM PAYMENT
                      WHERE AMOUNT BETWEEN 10000 AND 40000
                      """,

    "products_low_price_stock": """
                                SELECT SUM(STOCK_QUANTITY)
                                FROM PRODUCT
                                WHERE PRICE < 100
                                """,

    "invoices_with_customers": """
                               SELECT INVOICE.INVOICE_NUMBER, INVOICE.TOTAL_AMOUNT, CUSTOMER.NAME
                               FROM INVOICE
                                        INNER JOIN CUSTOMER ON INVOICE.CUSTOMER_ID = CUSTOMER.CUSTOMER_ID
                               """,

    "invoices_with_payments": """
                              SELECT I.INVOICE_ID, I.INVOICE_NUMBER, P.METHOD, P.AMOUNT
                              FROM INVOICE AS I
                                       LEFT JOIN PAYMENT AS P ON I.INVOICE_ID = P.INVOICE_ID
                              ORDER BY I.INVOICE_ID
                              """,

    # Raport: Ilość sztuk per kraj
    "report_quantity_per_country": """
                                   SELECT c.COUNTRY        AS Kraj,
                                          p.NAME           AS Nazwa_Produktu,
                                          SUM(oi.QUANTITY) AS Laczna_Ilosc_Sztuk
                                   FROM CUSTOMER c
                                            JOIN CUSTOMER_ORDER co ON c.CUSTOMER_ID = co.CUSTOMER_ID
                                            JOIN ORDER_ITEM oi ON co.ORDER_ID = oi.ORDER_ID
                                            JOIN PRODUCT p ON oi.PRODUCT_ID = p.PRODUCT_ID
                                   GROUP BY c.COUNTRY,
                                            p.NAME
                                   ORDER BY c.COUNTRY ASC,
                                            Laczna_Ilosc_Sztuk DESC
                                   """,

    # Złożony raport sprzedażowy
    "complex_sales_report": """
                            SELECT c.NAME                           AS Nazwa_Klienta,
                                   c.COUNTRY                        AS Kraj,
                                   u.USERNAME                       AS Agent,
                                   COUNT(DISTINCT co.ORDER_ID)      AS Liczba_Zrealizowanych_Zamowien,
                                   COUNT(DISTINCT p.PRODUCT_ID)     AS Liczba_Unikalnych_Produktow,
                                   SUM(oi.QUANTITY)                 AS Laczna_Ilosc_Sztuk,
                                   SUM(oi.QUANTITY * oi.UNIT_PRICE) AS Wartosc_Zamowien_Brutto,
                                   MAX(i.ISSUE_DATE)                AS Data_Ostatniej_Faktury
                            FROM CUSTOMER c
                                     JOIN CUSTOMER_ORDER co ON c.CUSTOMER_ID = co.CUSTOMER_ID
                                     JOIN ORDER_ITEM oi ON co.ORDER_ID = oi.ORDER_ID
                                     JOIN PRODUCT p ON oi.PRODUCT_ID = p.PRODUCT_ID
                                     JOIN INVOICE i ON co.ORDER_ID = i.ORDER_ID
                                     JOIN SYS_USER u ON i.CREATED_BY = u.USER_ID
                                     JOIN PAYMENT pay ON i.INVOICE_ID = pay.INVOICE_ID
                            WHERE co.STATUS = 'COMPLETED'
                              AND pay.CONFIRMED = 1
                            GROUP BY c.NAME,
                                     c.COUNTRY,
                                     u.USERNAME
                            HAVING SUM(oi.QUANTITY) > 10
                            ORDER BY Wartosc_Zamowien_Brutto DESC
                            """
}

# ==========================================
# ZAPYTANIA MODYFIKUJACE STRUKTURE (DDL)
# ==========================================
DDL_QUERIES = {
    "add_column_past_due": """
                           ALTER TABLE INVOICE
                               ADD PAST_DUE SMALLINT DEFAULT 0
                           """
    # Uwaga: Firebird często używa SMALLINT (0/1) zamiast typu BOOL w starszych wersjach,
    # ale w 3.0+ jest BOOLEAN. Zostawiłem zgodne z Twoim schematem.
}

# ==========================================
# ZAPYTANIA AKTUALIZUJACE (UPDATE/DELETE)
# ==========================================
DML_QUERIES = {
    "mark_past_due": """
                     UPDATE INVOICE as I
                     SET I.PAST_DUE = 1
                     WHERE DUE_DATE < '2023-12-31'
                     """,

    # Poprawione pod Firebird SQL (UPDATE z JOIN w Firebird robi się przez MERGE lub WHERE EXISTS)
    "reset_amount_for_apolonia": """
                                 UPDATE CUSTOMER_ORDER CO
                                 SET TOTAL_AMOUNT = 0
                                 WHERE EXISTS (SELECT 1
                                               FROM CUSTOMER C
                                               WHERE C.CUSTOMER_ID = CO.CUSTOMER_ID
                                                 AND C.NAME = 'Apolonia Banak')
                                 """,

    "delete_specific_item": """
                            DELETE
                            FROM ORDER_ITEM
                            WHERE PRODUCT_ID = 2
                            """,

    # Najpierw usuwamy powiazane ORDER_ITEM, potem PRODUCT
    "delete_order_items_for_products": """
                           DELETE FROM ORDER_ITEM
                           """,

    "delete_all_products": """
                           DELETE
                           FROM PRODUCT
                           """
}

# ==========================================
# INSERTY Z PLACEHOLDERAMI
# ==========================================
# Użycie: INSERT_TEMPLATES['customer'].format(name="Jan", email="jan@o2.pl", ...)

INSERT_TEMPLATES = {
    "customer": """
                INSERT INTO CUSTOMER (NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY)
                VALUES ('{name}', '{email}', '{phone}', '{address}', '{city}', '{country}')
                """,

    "product": """
               INSERT INTO PRODUCT (NAME, DESCRIPTION, PRICE, STOCK_QUANTITY)
               VALUES ('{name}', '{description}', {price}, {stock_quantity})
               """
}