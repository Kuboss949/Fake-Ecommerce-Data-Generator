#import i inicjalizowanie połączenia z orient 1.5.6
import pyorientdb as pyorient       
orient_engine = pyorient.OrientDB("localhost", 2424)
orient_engine.set_session_token(True)
orient_session = orient_engine.connect("root", "root")
orient_engine.db_open( 'company', "root", "root" )


orient_engine.command("create class  CUSTOMER extends V")
orient_engine.command("create class  CUSTOMER_ORDER extends V")
orient_engine.command("create class  INVOICE extends V")
orient_engine.command("create class  ORDER_ITEM extends V")
orient_engine.command("create class  PAYMENT extends V")
orient_engine.command("create class  PRODUCT extends V")
orient_engine.command("create class SYS_USER extends V")


orient_engine.command("CREATE CLASS Customer_to_invoice EXTENDS E;")
orient_engine.command("CREATE CLASS Invoice_to_payment EXTENDS E;")
orient_engine.command("CREATE CLASS Customer_to_order EXTENDS E;")
orient_engine.command("CREATE CLASS Order_to_invoice EXTENDS E;")
orient_engine.command("CREATE CLASS User_to_invoice EXTENDS E;")
orient_engine.command("CREATE CLASS Order_to_order_item EXTENDS E;")
orient_engine.command("CREATE CLASS Product_to_order_item EXTENDS E;")

orient_engine.command('CREATE FUNCTION fill_edge "LET customers = (SELECT FROM CUSTOMER); FOREACH (c IN $customers) {CREATE EDGE Customer_to_invoice FROM $c TO (SELECT FROM INVOICE WHERE CUSTOMER_ID = $c.CUSTOMER_ID);}\
LET invoices = (SELECT FROM INVOICE); FOREACH (i IN $invoices) { CREATE EDGE Invoice_to_payment FROM $i TO (SELECT FROM PAYMENT WHERE INVOICE_ID = $i.INVOICE_ID);}\
LET customers = (SELECT FROM CUSTOMER); FOREACH (c IN $customers) {CREATE EDGE Customer_to_order FROM $c TO (SELECT FROM CUSTOMER_ORDER WHERE CUSTOMER_ID = $c.CUSTOMER_ID);}\
LET orders = (SELECT FROM CUSTOMER_ORDER); FOREACH (o IN $orders) {CREATE EDGE Order_to_invoice FROM $o TO (SELECT FROM INVOICE WHERE ORDER_ID = $o.ORDER_ID);}\
LET users = (SELECT FROM SYS_USER); FOREACH (u IN $users) {CREATE EDGE User_to_invoice FROM $u TO (SELECT FROM INVOICE WHERE CREATED_BY = $u.USER_ID);}\
LET orders = (SELECT FROM CUSTOMER_ORDER); FOREACH (o IN $orders) {CREATE EDGE Order_to_order_item FROM $o TO (SELECT FROM ORDER_ITEM WHERE ORDER_ID = $o.ORDER_ID);}\
LET products = (SELECT FROM PRODUCT); FOREACH (p IN $products) {CREATE EDGE Product_to_order_item FROM $p TO (SELECT FROM ORDER_ITEM WHERE PRODUCT_ID = $p.PRODUCT_ID);}"\
                LANGUAGE sql')

orient_engine.command('CREATE FUNCTION drop_all_data "DELETE FROM PRODUCT UNSAFE; DELETE FROM CUSTOMER UNSAFE; DELETE FROM SYS_USER UNSAFE; DELETE FROM CUSTOMER_ORDER UNSAFE;\
DELETE FROM PAYMENT UNSAFE; DELETE FROM ORDER_ITEM UNSAFE; DELETE FROM INVOICE UNSAFE; DELETE FROM Customer_to_invoice UNSAFE; DELETE FROM Invoice_to_payment UNSAFE;\
DELETE FROM Customer_to_order UNSAFE; DELETE FROM Order_to_invoice UNSAFE; DELETE FROM User_to_invoice UNSAFE; DELETE FROM Order_to_order_item UNSAFE;\
DELETE FROM Product_to_order_item UNSAFE;"\
            LANGUAGE sql')
