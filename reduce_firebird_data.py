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
    Wersja z batchowaniem, aby uniknac bledu limitu 65k w IN clause.
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

    orders_to_delete_count = current_orders - target_orders
    print(f"\n[3/8] Do usuniecia: {orders_to_delete_count:,} zamowien")

    # Znajdz ORDER_ID do usuniecia (najstarsze)
    print("\n[4/8] Wyszukiwanie zamowien do usuniecia...")
    # Pobieramy same ID
    result = session.execute(text(f"""
        SELECT FIRST {orders_to_delete_count} ORDER_ID
        FROM CUSTOMER_ORDER
        ORDER BY ORDER_ID ASC
    """))
    all_order_ids = [row[0] for row in result]

    if not all_order_ids:
        print("[INFO] Brak zamowien do usuniecia.")
        session.close()
        return

    # --- ZMIANA: Przetwarzanie w paczkach (batches) ---
    BATCH_SIZE = 1000  # Bezpieczna wartosc ponizej limitu 65535
    total_deleted_orders = 0

    print(f"\n[5-6/8] Usuwanie danych w paczkach po {BATCH_SIZE}...")

    # Pętla po paczkach ID
    for i in range(0, len(all_order_ids), BATCH_SIZE):
        batch_ids = all_order_ids[i: i + BATCH_SIZE]
        order_ids_str = ','.join(map(str, batch_ids))

        # 1. Znajdz faktury dla tej paczki zamowien
        result_inv = session.execute(text(f"""
            SELECT INVOICE_ID FROM INVOICE WHERE ORDER_ID IN ({order_ids_str})
        """))
        invoice_ids = [row[0] for row in result_inv]
        invoice_ids_str = ','.join(map(str, invoice_ids)) if invoice_ids else '0'

        # 2. Usun PAYMENT (dla faktur z tej paczki)
        if invoice_ids:
            session.execute(text(f"DELETE FROM PAYMENT WHERE INVOICE_ID IN ({invoice_ids_str})"))

        # 3. Usun INVOICE (dla faktur z tej paczki)
        if invoice_ids:
            session.execute(text(f"DELETE FROM INVOICE WHERE INVOICE_ID IN ({invoice_ids_str})"))

        # 4. Usun ORDER_ITEM (dla zamowien z tej paczki)
        session.execute(text(f"DELETE FROM ORDER_ITEM WHERE ORDER_ID IN ({order_ids_str})"))

        # 5. Usun CUSTOMER_ORDER (ta paczka)
        del_res = session.execute(text(f"DELETE FROM CUSTOMER_ORDER WHERE ORDER_ID IN ({order_ids_str})"))

        total_deleted_orders += del_res.rowcount

        # Wypisz postep co np. 5 paczek zeby nie spamowac konsoli
        if (i // BATCH_SIZE) % 5 == 0:
            print(f"   Postep: przetworzono {total_deleted_orders:,} / {orders_to_delete_count:,} zamowien...")

    print(f"   Zakonczono usuwanie. Razem usunieto: {total_deleted_orders:,}")

    # Commit
    print("\n[7/8] Zatwierdzanie zmian (COMMIT)...")
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
