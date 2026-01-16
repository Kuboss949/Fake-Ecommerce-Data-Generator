from SessionInitiator import SessionInitiator
from FakeDataGenerator import FakeDataGenerator
from benchmark import run_benchmark, ExecutionTimer
import codecs

# rejestracja kodowania cp1250, wyrzucało błąd dla firebirda
try:
    codecs.lookup('cp1250')
except LookupError:
    import encodings.cp1250


init = SessionInitiator(drop_data=True)

def main():
    print("="*70)
    print("🚀 SYSTEM GENEROWANIA DANYCH I BENCHMARKU BAZ DANYCH")
    print("="*70)

    # ==========================================
    # 🔧 INICJALIZACJA BAZ DANYCH
    # ==========================================
    print("\n📌 FAZA 1: INICJALIZACJA BAZ DANYCH")
    print("-"*70)

    with ExecutionTimer("Inicjalizacja Firebird"):
        session_fb = init.initiate_fb()

    with ExecutionTimer("Inicjalizacja MariaDB"):
        session_maria = init.initiate_maria()

    with ExecutionTimer("Inicjalizacja OrientDB"):
        orient_client = init.initiate_orient()

    with ExecutionTimer("Inicjalizacja Cassandra"):
        init.initiate_cassandra()

    # ==========================================
    # 🎲 GENEROWANIE DANYCH
    # ==========================================
    print("\n📌 FAZA 2: GENEROWANIE DANYCH")
    print("-"*70)

    generator = FakeDataGenerator(
        main_session=session_fb,
        mirror_session=session_maria,
        orient_client=orient_client
    )

    with ExecutionTimer("Generowanie i replikacja danych"):
        generator.run_generation(
            num_users=10,
            num_customers=50,
            num_orders=100
        )

    # ==========================================
    # 📊 BENCHMARK BAZ DANYCH
    # ==========================================
    print("\n📌 FAZA 3: BENCHMARK WYDAJNOŚCI")
    print("-"*70)

    csv_path = run_benchmark(
        fb_session=session_fb,
        maria_session=session_maria,
        orient_client=orient_client
    )

    # ==========================================
    # ✅ PODSUMOWANIE
    # ==========================================
    print("\n" + "="*70)
    print("✅ WSZYSTKIE OPERACJE ZAKOŃCZONE POMYŚLNIE")
    print("="*70)
    if csv_path:
        print(f"📄 Wyniki benchmarku zapisane w: {csv_path}")
    print("🔒 Sesje pozostają otwarte dla dalszych testów.")
    print("="*70)


if __name__ == '__main__':
    main()

