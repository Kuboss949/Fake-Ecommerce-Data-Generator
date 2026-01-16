#!/usr/bin/env python3
# benchmark.py

"""
Benchmark dla porównania wydajności baz danych:
- Firebird
- MariaDB
- OrientDB
- Cassandra

Wykonuje zapytania SELECT, DDL i DML, mierzy czas wykonania
i zapisuje wyniki do plików CSV.
"""

import time
import csv
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import text
from cassandra.cluster import Cluster

# Importy plików z zapytaniami
from firebird_query import SELECT_QUERIES as FB_SELECT, DDL_QUERIES as FB_DDL, DML_QUERIES as FB_DML
from mariadb_query import SELECT_QUERIES as MARIA_SELECT, DDL_QUERIES as MARIA_DDL, DML_QUERIES as MARIA_DML
from orientdb_queries import (
    ORIENT_SELECT_QUERIES, ORIENT_DDL_QUERIES, ORIENT_DML_QUERIES,
    execute_orient_query, execute_orient_command
)
from cassandra_queries import (
    CQL_SELECT_QUERIES, CQL_DDL_QUERIES, CQL_DML_QUERIES
)


# ==========================================
# EXECUTION TIMER - CONTEXT MANAGER
# ==========================================

@contextmanager
def ExecutionTimer(name="Operation"):
    """
    Context manager do mierzenia czasu wykonania.

    Usage:
        with ExecutionTimer("My operation"):
            # kod do zmierzenia
    """
    print(f"[TIMER] {name}...", end=" ", flush=True)
    start_time = time.time()
    yield
    end_time = time.time()
    elapsed = (end_time - start_time) * 1000  # ms
    print(f"OK {elapsed:.2f}ms")


# ==========================================
# BENCHMARK RESULTS STORAGE
# ==========================================

benchmark_results = []


# ==========================================
# FUNKCJA DO POLACZENIA Z CASSANDRA
# ==========================================

def connect_cassandra(keyspace='my_keyspace', nodes=['127.0.0.1']):
    """Polaczenie z Cassandra - tylko ta baza potrzebuje wlasnego polaczenia"""
    print("[Cassandra] Laczenie z Cassandra...")
    cluster = Cluster(nodes)
    session = cluster.connect(keyspace)
    return session, cluster


# ==========================================
# FUNKCJE POBIERAJACE PRZYKLADOWE WARTOSCI
# ==========================================

def get_sample_values(fb_session, cassandra_session):
    """
    Pobiera przykładowe wartości z baz danych do użycia w zapytaniach parametryzowanych.

    Returns:
        dict: Słownik z przykładowymi wartościami
    """
    sample_values = {}

    try:
        # Pobierz przykladowe miasto z Firebird (FIRST 1 zamiast LIMIT 1)
        result = fb_session.execute(text("SELECT FIRST 1 CITY FROM CUSTOMER"))
        row = result.fetchone()
        sample_values['city'] = row[0] if row else 'Warszawa'

        # Pobierz przykladowy rok (z daty platnosci)
        result = fb_session.execute(text("SELECT FIRST 1 EXTRACT(YEAR FROM PAYMENT_DATE) as year FROM PAYMENT"))
        row = result.fetchone()
        sample_values['year'] = int(row[0]) if row else 2024

        # Pobierz przykladowy kraj
        result = fb_session.execute(text("SELECT FIRST 1 COUNTRY FROM CUSTOMER"))
        row = result.fetchone()
        sample_values['country'] = row[0] if row else 'Polska'

        # Pobierz przykladowe invoice_id
        result = fb_session.execute(text("SELECT FIRST 1 INVOICE_ID FROM INVOICE"))
        row = result.fetchone()
        sample_values['invoice_id'] = int(row[0]) if row else 1

        # Pobierz przykladowa nazwe klienta
        result = fb_session.execute(text("SELECT FIRST 1 NAME FROM CUSTOMER"))
        row = result.fetchone()
        sample_values['customer_name'] = row[0] if row else 'Test Customer'

        # Pobierz przykladowe product_id
        result = fb_session.execute(text("SELECT FIRST 1 PRODUCT_ID FROM PRODUCT"))
        row = result.fetchone()
        sample_values['product_id'] = int(row[0]) if row else 1

        print(f"   Pobrano przykladowe wartosci: city={sample_values['city']}, year={sample_values['year']}, country={sample_values['country']}")

    except Exception as e:
        print(f"   [WARN] Blad podczas pobierania przykladowych wartosci: {e}")
        # Domyślne wartości
        sample_values = {
            'city': 'Warszawa',
            'year': 2024,
            'country': 'Polska',
            'invoice_id': 1,
            'customer_name': 'Test Customer',
            'product_id': 1
        }

    return sample_values


# ==========================================
# FUNKCJE BENCHMARKOWE
# ==========================================

def benchmark_sql_query(session, query_name, query_text, db_name):
    """
    Wykonuje zapytanie SQL (Firebird/MariaDB) i mierzy czas.

    Args:
        session: Sesja SQLAlchemy
        query_name: Nazwa zapytania
        query_text: Tekst zapytania SQL
        db_name: Nazwa bazy danych (Firebird/MariaDB)

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   {db_name}: {query_name}...", end=" ")

    try:
        start_time = time.time()
        result = session.execute(text(query_text))
        rows = result.fetchall()
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms
        row_count = len(rows)

        print(f"OK {execution_time:.2f}ms ({row_count} rows)")

        return {
            "database": db_name,
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": row_count,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": db_name,
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_sql_command(session, query_name, query_text, db_name, query_type="DML"):
    """
    Wykonuje komendę SQL (UPDATE/DELETE/ALTER) i mierzy czas.

    Args:
        session: Sesja SQLAlchemy
        query_name: Nazwa zapytania
        query_text: Tekst zapytania SQL
        db_name: Nazwa bazy danych
        query_type: Typ zapytania (DDL/DML)

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   {db_name}: {query_name}...", end=" ")

    try:
        start_time = time.time()
        result = session.execute(text(query_text))
        session.commit()
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms")

        return {
            "database": db_name,
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": None,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        session.rollback()
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": db_name,
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_orient_query(client, query_name, query_text):
    """
    Wykonuje zapytanie OrientDB i mierzy czas.

    Args:
        client: Klient pyorient
        query_name: Nazwa zapytania
        query_text: Tekst zapytania

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   OrientDB: {query_name}...", end=" ")

    try:
        start_time = time.time()
        result = execute_orient_query(client, query_text)
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms
        row_count = len(result)

        print(f"OK {execution_time:.2f}ms ({row_count} rows)")

        return {
            "database": "OrientDB",
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": row_count,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "OrientDB",
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_orient_command(client, query_name, query_text, query_type="DML"):
    """
    Wykonuje komendę OrientDB (UPDATE/DELETE/ALTER) i mierzy czas.

    Args:
        client: Klient pyorient
        query_name: Nazwa zapytania
        query_text: Tekst zapytania
        query_type: Typ zapytania (DDL/DML)

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   OrientDB: {query_name}...", end=" ")

    try:
        start_time = time.time()
        execute_orient_command(client, query_text)
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms")

        return {
            "database": "OrientDB",
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": None,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "OrientDB",
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_cassandra_query(session, query_name, query_text):
    """
    Wykonuje zapytanie Cassandra i mierzy czas.

    Args:
        session: Sesja Cassandra
        query_name: Nazwa zapytania
        query_text: Tekst zapytania CQL

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   Cassandra: {query_name}...", end=" ")

    try:
        start_time = time.time()
        result = session.execute(query_text)
        rows = list(result)
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms
        row_count = len(rows)

        print(f"OK {execution_time:.2f}ms ({row_count} rows)")

        return {
            "database": "Cassandra",
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": row_count,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "Cassandra",
            "query_type": "SELECT",
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_cassandra_command(session, query_name, query_text, query_type="DML"):
    """
    Wykonuje komendę Cassandra (UPDATE/DELETE/ALTER) i mierzy czas.

    Args:
        session: Sesja Cassandra
        query_name: Nazwa zapytania
        query_text: Tekst zapytania CQL
        query_type: Typ zapytania (DDL/DML)

    Returns:
        Dict z wynikami benchmarku
    """
    print(f"   Cassandra: {query_name}...", end=" ")

    try:
        start_time = time.time()
        session.execute(query_text)
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms")

        return {
            "database": "Cassandra",
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": round(execution_time, 2),
            "row_count": None,
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "Cassandra",
            "query_type": query_type,
            "query_name": query_name,
            "execution_time_ms": None,
            "row_count": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


# ==========================================
# FUNKCJE GLOWNE BENCHMARKU
# ==========================================

def run_select_benchmarks(fb_session, maria_session, orient_client, cassandra_session):
    """Uruchamia benchmarki dla zapytan SELECT"""
    print("\n" + "="*60)
    print("BENCHMARK: SELECT QUERIES")
    print("="*60)

    # Pobierz przykładowe wartości do parametryzowanych zapytań
    sample_values = get_sample_values(fb_session, cassandra_session)

    # Firebird SELECT
    print("\n[Firebird] SELECT:")
    for name, query in FB_SELECT.items():
        result = benchmark_sql_query(fb_session, name, query, "Firebird")
        benchmark_results.append(result)

    # MariaDB SELECT
    print("\n[MariaDB] SELECT:")
    for name, query in MARIA_SELECT.items():
        result = benchmark_sql_query(maria_session, name, query, "MariaDB")
        benchmark_results.append(result)

    # OrientDB SELECT
    print("\n[OrientDB] SELECT:")
    for name, query in ORIENT_SELECT_QUERIES.items():
        result = benchmark_orient_query(orient_client, name, query)
        benchmark_results.append(result)

    # Cassandra SELECT - z podstawionymi parametrami
    print("\n[Cassandra] SELECT:")

    for name, query_template in CQL_SELECT_QUERIES.items():
        try:
            # Podstaw parametry do zapytania
            query = query_template.format(**sample_values)
            result = benchmark_cassandra_query(cassandra_session, name, query)
            benchmark_results.append(result)
        except KeyError as e:
            print(f"   [WARN] Pominieto {name} - brak parametru {e}")
        except Exception as e:
            print(f"   [WARN] Blad w {name}: {e}")


def run_ddl_benchmarks(fb_session, maria_session, orient_client, cassandra_session):
    """Uruchamia benchmarki dla zapytan DDL (ALTER TABLE)"""
    print("\n" + "="*60)
    print("BENCHMARK: DDL QUERIES (ALTER TABLE)")
    print("="*60)

    # Firebird DDL
    print("\n[Firebird] DDL:")
    for name, query in FB_DDL.items():
        result = benchmark_sql_command(fb_session, name, query, "Firebird", "DDL")
        benchmark_results.append(result)

    # MariaDB DDL
    print("\n[MariaDB] DDL:")
    for name, query in MARIA_DDL.items():
        result = benchmark_sql_command(maria_session, name, query, "MariaDB", "DDL")
        benchmark_results.append(result)

    # OrientDB DDL
    print("\n[OrientDB] DDL:")
    for name, query in ORIENT_DDL_QUERIES.items():
        result = benchmark_orient_command(orient_client, name, query, "DDL")
        benchmark_results.append(result)

    # Cassandra DDL
    print("\n[Cassandra] DDL:")
    for name, query in CQL_DDL_QUERIES.items():
        result = benchmark_cassandra_command(cassandra_session, name, query, "DDL")
        benchmark_results.append(result)


def run_dml_benchmarks(fb_session, maria_session, orient_client, cassandra_session):
    """Uruchamia benchmarki dla zapytan DML (UPDATE/DELETE)"""
    print("\n" + "="*60)
    print("BENCHMARK: DML QUERIES (UPDATE/DELETE)")
    print("="*60)

    # Pobierz przykładowe wartości do parametryzowanych zapytań
    sample_values = get_sample_values(fb_session, cassandra_session)

    # Firebird DML
    print("\n[Firebird] DML:")
    for name, query in FB_DML.items():
        result = benchmark_sql_command(fb_session, name, query, "Firebird", "DML")
        benchmark_results.append(result)

    # MariaDB DML
    print("\n[MariaDB] DML:")
    for name, query in MARIA_DML.items():
        result = benchmark_sql_command(maria_session, name, query, "MariaDB", "DML")
        benchmark_results.append(result)

    # OrientDB DML
    print("\n[OrientDB] DML:")
    for name, query in ORIENT_DML_QUERIES.items():
        result = benchmark_orient_command(orient_client, name, query, "DML")
        benchmark_results.append(result)

    # Cassandra DML - z podstawionymi parametrami
    print("\n[Cassandra] DML:")

    for name, query_template in CQL_DML_QUERIES.items():
        try:
            # Podstaw parametry do zapytania
            query = query_template.format(**sample_values)
            result = benchmark_cassandra_command(cassandra_session, name, query, "DML")
            benchmark_results.append(result)
        except KeyError as e:
            print(f"   [WARN] Pominieto {name} - brak parametru {e}")
        except Exception as e:
            print(f"   [WARN] Blad w {name}: {e}")


# ==========================================
# FUNKCJE ZAPISU WYNIKOW
# ==========================================

def save_results_to_csv():
    """
    Zapisuje wyniki benchmarku do pliku CSV.

    Returns:
        str: Ścieżka do zapisanego pliku CSV
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}.csv"

    print(f"\n[SAVE] Zapisywanie wynikow do: {filename}")

    fieldnames = [
        "database",
        "query_type",
        "query_name",
        "execution_time_ms",
        "row_count",
        "status",
        "error"
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(benchmark_results)

    print(f"OK Zapisano {len(benchmark_results)} wynikow do {filename}")
    return filename


def print_summary():
    """Wyswietla podsumowanie wynikow benchmarku"""
    print("\n" + "="*60)
    print("PODSUMOWANIE BENCHMARKU")
    print("="*60)

    databases = set(r["database"] for r in benchmark_results)

    for db in databases:
        db_results = [r for r in benchmark_results if r["database"] == db]
        success_count = len([r for r in db_results if r["status"] == "SUCCESS"])
        error_count = len([r for r in db_results if r["status"] == "ERROR"])

        avg_time = None
        if success_count > 0:
            times = [r["execution_time_ms"] for r in db_results if r["execution_time_ms"] is not None]
            if times:
                avg_time = sum(times) / len(times)

        print(f"\n{db}:")
        print(f"   Sukces: {success_count}")
        print(f"   Bledy: {error_count}")
        if avg_time:
            print(f"   Sredni czas: {avg_time:.2f}ms")


# ==========================================
# MAIN
# ==========================================

def run_benchmark(fb_session, maria_session, orient_client):
    """
    Główna funkcja benchmarku - przyjmuje sesje z zewnątrz.

    Args:
        fb_session: Sesja Firebird
        maria_session: Sesja MariaDB
        orient_client: Klient OrientDB

    Returns:
        str: Ścieżka do pliku CSV z wynikami
    """
    print("\n" + "="*60)
    print("BENCHMARK BAZ DANYCH - START")
    print("="*60)

    # Połączenie tylko z Cassandrą (inne sesje są z parametrów)
    cassandra_session, cassandra_cluster = connect_cassandra()

    try:
        # Uruchomienie benchmarków
        with ExecutionTimer("SELECT benchmarks"):
            run_select_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        with ExecutionTimer("DDL benchmarks"):
            run_ddl_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        with ExecutionTimer("DML benchmarks"):
            run_dml_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        # Podsumowanie i zapis
        print_summary()
        csv_path = save_results_to_csv()

        print("\n" + "="*60)
        print("BENCHMARK BAZ DANYCH - KONIEC")
        print("="*60)

        return csv_path

    except Exception as e:
        print(f"\n[ERROR] KRYTYCZNY BLAD: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        # Zamkniecie tylko Cassandry (inne sesje sa zarzadzane w main.py)
        print("\n[Cassandra] Zamykanie polaczenia...")
        cassandra_session.shutdown()
        cassandra_cluster.shutdown()


if __name__ == "__main__":
    print("[ERROR] Ten skrypt powinien byc wywolywany z main.py, nie bezposrednio!")
    print("Uzyj: python main.py")
