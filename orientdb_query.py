import pyorientdb as pyorient 

#ile wyników zapytań zostanie zapisanych
counter = 100

orient_engine = pyorient.OrientDB("localhost", 2424)
orient_engine.set_session_token(True)
session_id = orient_engine.connect("root", "root")
orient_engine.db_open( 'company', "root", "root" )

print("Zapytanie 1")
result1 = orient_engine.query("SELECT NAME , EMAIL FROM SYS_USER WHERE ROLE  = 'ACCOUNTANT'",counter,"*:-1")
for account in result1:
    print(account.NAME)
    print(account.EMAIL)

print("Zapytanie 2")
result2 = orient_engine.query("SELECT COUNT(*) AS NUMBER, CITY  FROM CUSTOMER GROUP BY CITY ",counter,"*:-1")
for city in result2:
    print(city.NUMBER)
    print(city.CITY)

print("Zapytanie 3")
result3 = orient_engine.query("SELECT FROM PAYMENT WHERE AMOUNT BETWEEN 10000 AND 40000")
for payment in result3:
    print(payment.PAYMENT_ID)
    print(payment.METHOD)
    print(payment.PAYMENT_DATE)
    print(payment.AMOUNT)
    print(payment.CONFIRMED)

print("Zapytanie 4")
result4 = orient_engine.query("SELECT SUM(STOCK_QUANTITY) AS SUMMARY FROM PRODUCT WHERE PRICE < 500",counter,"*:-1")
for stock in result4:
    print(stock.SUMMARY)

print("Zapytanie 5")
result5 = orient_engine.query("MATCH {class: CUSTOMER, as: c} -Customer_to_invoice-> {class: INVOICE, as: i} RETURN i.CUSTOMER_ID as id_klienta, i.INVOICE_NUMBER as numer_faktury, c.NAME as nazwa",counter,"*:-1")
for invoice in result5:
    print(invoice.id_klienta)
    print(invoice.numer_faktury)
    print(invoice.nazwa)


print("Zapytanie 6")
result6 = orient_engine.query("MATCH {class: INVOICE, as: i} -Invoice_to_payment-> {class: PAYMENT, as: p}RETURN i.INVOICE_ID as id_klienta, i.INVOICE_NUMBER as numer_faktury , p.METHOD  as metoda, p.AMOUNT as ilosc_zamowien ORDER BY i.INVOICE_ID",counter,"*:-1")
for invoice in result6:
    print(invoice.id_klienta)
    print(invoice.numer_faktury)
    print(invoice.metoda)
    print(invoice.ilosc_zamowien)


print("Zapytanie 7 nie musi być wykonywane ze względu na nieschematycze działanie OrientDB, pozwaljące dodawać nowe pola  od ręki")

print("Zapytanie 8")
result8 = orient_engine.command("UPDATE INVOICE SET PAST_DUE = 1 WHERE DUE_DATE < '2023-12-31'")
print("Wykonano zapytanie 8")

print("Zapytanie 9")
result9 = orient_engine.command("UPDATE CUSTOMER_ORDER MERGE {'TOTAL_AMOUNT': 0} WHERE @rid IN (SELECT expand(o) FROM (MATCH {class: CUSTOMER, as: c, where: (NAME = 'Oliwier Tusk')} -Customer_to_order-> {class: CUSTOMER_ORDER, as: o}RETURN o));",counter,"*:-1")
print("Wykonano zapytanie 9")

print("Zapytanie 10")
result10 = orient_engine.command("DELETE VERTEX FROM ORDER_ITEM WHERE PRODUCT_ID = 2",counter,"*:-1")
print("Wykonano zapytanie 10")

print("Zapytanie 11")
result11 = orient_engine.query("MATCH {class: CUSTOMER, as: c} -Customer_to_order-> {class: CUSTOMER_ORDER, as: co} -Order_to_order_item-> {class: ORDER_ITEM, as: o} <-Product_to_order_item- {class: PRODUCT, as: p}RETURN c.COUNTRY as panstwo , SUM(o.QUANTITY) AS Laczna_Ilosc_Sztuk,p.NAME as nazwa GROUP BY c.COUNTRY, p.NAME ORDER BY c.COUNTRY ASC, Laczna_Ilosc_Sztuk DESC;",counter,"*:-1")
for customer in result11:
    print(customer.panstwo)
    print(customer.Laczna_Ilosc_Sztuk)
    print(customer.nazwa)

print("Zapytanie 12")
result12 = orient_engine.query("""MATCH 
                                {class: CUSTOMER, as: c} -Customer_to_order-> {class: CUSTOMER_ORDER, as: co, where: (STATUS = 'COMPLETED')} 
                                -Order_to_order_item-> {class: ORDER_ITEM, as: oi} <-Product_to_order_item- {class: PRODUCT, as: p}, 
                                {as: co} -Order_to_invoice-> {class: INVOICE, as: i} -Invoice_to_payment-> {class: PAYMENT, as: pay, where: (CONFIRMED = 1)}, 
                                {as: i} <-User_to_invoice- {class: SYS_USER, as: u} 
                                RETURN 
                                c.NAME 							AS Nazwa_Klienta,  
                                c.COUNTRY 						AS Kraj, 
                                u.USERNAME 						AS Agent, 
                                COUNT(DISTINCT(co.ORDER_ID)) 		AS Liczba_Zrealizowanych_Zamowien, 
                                COUNT(DISTINCT(p.PRODUCT_ID)) 	AS Liczba_Unikalnych_Produktow,
                                SUM(oi.QUANTITY) 					AS Laczna_Ilosc_Sztuk, 
                                SUM(oi.QUANTITY * oi.UNIT_PRICE)  AS Wartosc_Zamowien_Brutto, 
                                MAX(i.ISSUE_DATE) 				AS Data_Ostatniej_Faktury
  
                                GROUP BY c.NAME, c.COUNTRY, u.USERNAME
                                ORDER BY 
                                    Wartosc_Zamowien_Brutto DESC;
                                """,counter,"*:-1")
for customer in result12:
    print(customer.Nazwa_Klienta)
    print(customer.Kraj)
    print(customer.Agent)
    print(customer.Liczba_Zrealizowanych_Zamowien)
    print(customer.Liczba_Unikalnych_Produktow)
    print(customer.Laczna_Ilosc_Sztuk)
    print(customer.Wartosc_Zamowien_Brutto)
    print(customer.Data_Ostatniej_Faktury)


orient_engine.db_close()