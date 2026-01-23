from SessionInitiator import SessionInitiator
from FakeDataGenerator import FakeDataGenerator
from benchmark import run_benchmark, ExecutionTimer
import codecs

# rejestracja kodowania cp1250, wyrzucało błąd dla firebirda
try:
    codecs.lookup('cp1250')
except LookupError:
    import encodings.cp1250


init = SessionInitiator(drop_data=False)

def main():
    print("="*70)
    print("SYSTEM GENEROWANIA DANYCH I BENCHMARKU BAZ DANYCH")
    print("="*70)

    # ==========================================
    # INICJALIZACJA BAZ DANYCH
    # ==========================================
    print("\n[FAZA 1] INICJALIZACJA BAZ DANYCH")
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
    # GENEROWANIE DANYCH
    # ==========================================
    # print("\n[FAZA 2] GENEROWANIE DANYCH")
    # print("-"*70)
    #
    # generator = FakeDataGenerator(
    #     main_session=session_fb,
    #     mirror_session=session_maria,
    #     orient_client=orient_client
    # )
    #
    # with ExecutionTimer("Generowanie i replikacja danych"):
    #     generator.run_generation(
    #         num_users=1000,
    #         num_customers=5000,
    #         num_orders=95000
    #     )


    # ==========================================
    # BENCHMARK BAZ DANYCH
    # ==========================================
    print("\n[FAZA 3] BENCHMARK WYDAJNOSCI")
    print("-"*70)

    csv_path = run_benchmark(
        fb_session=session_fb,
        maria_session=session_maria,
        orient_client=orient_client
    )

    # ==========================================
    # PODSUMOWANIE
    # ==========================================
    print("\n" + "="*70)
    print("WSZYSTKIE OPERACJE ZAKONCZONE POMYSLNIE")
    print("="*70)
    if csv_path:
        print(f"Wyniki benchmarku zapisane w: {csv_path}")
    print("Sesje pozostaja otwarte dla dalszych testow.")
    print("="*70)


if __name__ == '__main__':
    main()

