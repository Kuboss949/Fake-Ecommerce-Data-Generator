"""
Skrypt do redukcji danych w Firebird.
Pozwala zmniejszyc ilosc rekordow w bazie do zadanej liczby zamowien.

Uzycie:
    python reduce_firebird_data.py 25000    # Zostaw 25k zamowien
    python reduce_firebird_data.py 2500     # Zostaw 2.5k zamowien

UWAGA: Operacja jest NIEODWRACALNA! Zrob kopie pliku bazy przed uruchomieniem.
"""

import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ============== KONFIGURACJA ==============
FIREBIRD_URL = "firebird+firebird://sysdba:admin123@localhost:3050//var/lib/firebird/data/mirror.fdb?charset=UTF8"


def get_current_counts(session):
    """Pobiera aktualne liczby rekordow w tabelach."""
    tables = ['SYS_USER', 'CUSTOMER', 'PRODUCT', 'CUSTOMER_ORDER', 'ORDER_ITEM', 'INVOICE', 'PAYMENT']
    counts = {}
    for table in tables:
        result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
        counts[table] = result.scalar()
    return counts


def reduce_data(target_orders: int):
    """
    Redukuje dane w Firebird do zadanej liczby zamowien.

    Args:
        target_orders: Docelowa liczba zamowien (CUSTOMER_ORDER)
    """
    print("=" * 60)
    print(f"REDUKCJA DANYCH FIREBIRD DO {target_orders} ZAMOWIEN")
    print("=" * 60)

    # Polacz z baza
    print("\n[1/8] Laczenie z Firebird...")
    engine = create_engine(FIREBIRD_URL, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Pokaz aktualne statystyki
    print("\n[2/8] Aktualne statystyki:")
    counts = get_current_counts(session)
    for table, count in counts.items():
        print(f"   {table}: {count:,} rekordow")

    current_orders = counts['CUSTOMER_ORDER']

    if current_orders <= target_orders:
        print(f"\n[INFO] Baza ma juz {current_orders:,} zamowien, nie ma co usuwac.")
        session.close()
        return

    orders_to_delete = current_orders - target_orders
    print(f"\n[3/8] Do usuniecia: {orders_to_delete:,} zamowien")

    # Znajdz ORDER_ID do usuniecia (najstarsze)
    print("\n[4/8] Wyszukiwanie zamowien do usuniecia...")
    result = session.execute(text(f"""
        SELECT FIRST {orders_to_delete} ORDER_ID
        FROM CUSTOMER_ORDER
        ORDER BY ORDER_ID ASC
    """))
    order_ids_to_delete = [row[0] for row in result]

    if not order_ids_to_delete:
        print("[INFO] Brak zamowien do usuniecia.")
        session.close()
        return

    # Konwertuj na string dla IN clause
    order_ids_str = ','.join(map(str, order_ids_to_delete))

    # Znajdz powiazane INVOICE_ID
    print("\n[5/8] Wyszukiwanie powiazanych faktur...")
    result = session.execute(text(f"""
        SELECT INVOICE_ID FROM INVOICE WHERE ORDER_ID IN ({order_ids_str})
    """))
    invoice_ids = [row[0] for row in result]
    invoice_ids_str = ','.join(map(str, invoice_ids)) if invoice_ids else '0'

    print(f"   Znaleziono {len(invoice_ids):,} faktur do usuniecia")

    # USUWANIE W KOLEJNOSCI FK
    print("\n[6/8] Usuwanie danych (kolejnosc FK)...")

    # 1. PAYMENT
    if invoice_ids:
        result = session.execute(text(f"DELETE FROM PAYMENT WHERE INVOICE_ID IN ({invoice_ids_str})"))
        print(f"   -> PAYMENT: usunieto {result.rowcount:,} rekordow")

    # 2. INVOICE
    if invoice_ids:
        result = session.execute(text(f"DELETE FROM INVOICE WHERE INVOICE_ID IN ({invoice_ids_str})"))
        print(f"   -> INVOICE: usunieto {result.rowcount:,} rekordow")

    # 3. ORDER_ITEM
    result = session.execute(text(f"DELETE FROM ORDER_ITEM WHERE ORDER_ID IN ({order_ids_str})"))
    print(f"   -> ORDER_ITEM: usunieto {result.rowcount:,} rekordow")

    # 4. CUSTOMER_ORDER
    result = session.execute(text(f"DELETE FROM CUSTOMER_ORDER WHERE ORDER_ID IN ({order_ids_str})"))
    print(f"   -> CUSTOMER_ORDER: usunieto {result.rowcount:,} rekordow")

    # Commit
    print("\n[7/8] Zatwierdzanie zmian...")
    session.commit()

    # Pokaz nowe statystyki
    print("\n[8/8] Nowe statystyki:")
    counts = get_current_counts(session)
    for table, count in counts.items():
        print(f"   {table}: {count:,} rekordow")

    session.close()

    print("\n" + "=" * 60)
    print("REDUKCJA ZAKONCZONA!")
    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nPrzykladowe uzycie:")
        print("  python reduce_firebird_data.py 25000   # dla ~250k rekordow")
        print("  python reduce_firebird_data.py 2500    # dla ~20k rekordow")
        print("  python reduce_firebird_data.py 95000   # dla ~1M rekordow (bez zmian)")
        sys.exit(1)

    try:
        target_orders = int(sys.argv[1])
    except ValueError:
        print(f"[ERROR] '{sys.argv[1]}' nie jest liczba!")
        sys.exit(1)

    if target_orders < 0:
        print("[ERROR] Liczba zamowien musi byc >= 0")
        sys.exit(1)

    # Potwierdzenie
    print(f"\n!!! UWAGA !!!")
    print(f"Ta operacja usunie dane z Firebird, zostawiajac tylko {target_orders:,} zamowien.")
    print(f"Operacja jest NIEODWRACALNA!")

    confirm = input("\nCzy kontynuowac? (tak/nie): ").strip().lower()
    if confirm != 'tak':
        print("Anulowano.")
        sys.exit(0)

    reduce_data(target_orders)


if __name__ == "__main__":
    main()
