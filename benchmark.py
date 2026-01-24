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
import uuid
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import text
from cassandra.cluster import Cluster
from faker import Faker

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
warmed_up_results = []
insert_benchmark_results = []

# Initialize Faker
fake = Faker('pl_PL')


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

        # Pobierz przykladowy rok (z daty platnosci) - YEAR jest slowem kluczowym, uzywamy innego aliasu
        result = fb_session.execute(text("SELECT FIRST 1 EXTRACT(YEAR FROM PAYMENT_DATE) AS payment_year FROM PAYMENT"))
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
        error_msg = str(e)
        # Dla DDL: jesli kolumna juz istnieje, traktujemy jako SKIPPED (nie blad)
        if query_type == "DDL" and "already exist" in error_msg.lower():
            print(f"SKIPPED (column exists)")
            return {
                "database": "Cassandra",
                "query_type": query_type,
                "query_name": query_name,
                "execution_time_ms": 0,
                "row_count": None,
                "status": "SUCCESS",
                "error": "Column already exists - skipped"
            }
        print(f"ERROR: {error_msg[:50]}")
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
# FUNKCJE WARM-UP I POMIAROWE
# ==========================================

def warm_up_database(fb_session, maria_session, orient_client, cassandra_session, sample_values):
    """
    Rozgrzewa bazy danych poprzez wykonanie wszystkich zapytan SELECT.
    """
    print("\n[WARM-UP] Rozgrzewanie baz danych...")

    # Firebird warm-up
    print("   Firebird...", end=" ")
    for name, query in FB_SELECT.items():
        try:
            result = fb_session.execute(text(query))
            result.fetchall()
        except:
            pass
    print("OK")

    # MariaDB warm-up
    print("   MariaDB...", end=" ")
    for name, query in MARIA_SELECT.items():
        try:
            result = maria_session.execute(text(query))
            result.fetchall()
        except:
            pass
    print("OK")

    # OrientDB warm-up
    print("   OrientDB...", end=" ")
    for name, query in ORIENT_SELECT_QUERIES.items():
        try:
            execute_orient_query(orient_client, query)
        except:
            pass
    print("OK")

    # Cassandra warm-up
    print("   Cassandra...", end=" ")
    for name, query_template in CQL_SELECT_QUERIES.items():
        try:
            query = query_template.format(**sample_values)
            result = cassandra_session.execute(query)
            list(result)
        except:
            pass
    print("OK")

    print("[WARM-UP] Rozgrzewanie zakonczone.\n")


def benchmark_sql_query_multiple(session, query_name, query_text, db_name, iterations=10):
    """
    Wykonuje zapytanie SQL wielokrotnie i zwraca sredni czas.
    """
    print(f"   {db_name}: {query_name} ({iterations}x)...", end=" ")

    times = []
    row_count = None

    for i in range(iterations):
        try:
            start_time = time.time()
            result = session.execute(text(query_text))
            rows = result.fetchall()
            end_time = time.time()

            execution_time = (end_time - start_time) * 1000  # ms
            times.append(execution_time)
            row_count = len(rows)

        except Exception as e:
            print(f"ERROR on iteration {i+1}: {str(e)[:50]}")
            return {
                "database": db_name,
                "query_type": "SELECT_WARMED_UP",
                "query_name": query_name,
                "avg_execution_time_ms": None,
                "min_execution_time_ms": None,
                "max_execution_time_ms": None,
                "iterations": iterations,
                "row_count": None,
                "status": "ERROR",
                "error": str(e)[:200]
            }

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"OK avg={avg_time:.2f}ms (min={min_time:.2f}, max={max_time:.2f}) ({row_count} rows)")

    return {
        "database": db_name,
        "query_type": "SELECT_WARMED_UP",
        "query_name": query_name,
        "avg_execution_time_ms": round(avg_time, 2),
        "min_execution_time_ms": round(min_time, 2),
        "max_execution_time_ms": round(max_time, 2),
        "iterations": iterations,
        "row_count": row_count,
        "status": "SUCCESS",
        "error": None
    }


def benchmark_orient_query_multiple(client, query_name, query_text, iterations=10):
    """
    Wykonuje zapytanie OrientDB wielokrotnie i zwraca sredni czas.
    """
    print(f"   OrientDB: {query_name} ({iterations}x)...", end=" ")

    times = []
    row_count = None

    for i in range(iterations):
        try:
            start_time = time.time()
            result = execute_orient_query(client, query_text)
            end_time = time.time()

            execution_time = (end_time - start_time) * 1000  # ms
            times.append(execution_time)
            row_count = len(result)

        except Exception as e:
            print(f"ERROR on iteration {i+1}: {str(e)[:50]}")
            return {
                "database": "OrientDB",
                "query_type": "SELECT_WARMED_UP",
                "query_name": query_name,
                "avg_execution_time_ms": None,
                "min_execution_time_ms": None,
                "max_execution_time_ms": None,
                "iterations": iterations,
                "row_count": None,
                "status": "ERROR",
                "error": str(e)[:200]
            }

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"OK avg={avg_time:.2f}ms (min={min_time:.2f}, max={max_time:.2f}) ({row_count} rows)")

    return {
        "database": "OrientDB",
        "query_type": "SELECT_WARMED_UP",
        "query_name": query_name,
        "avg_execution_time_ms": round(avg_time, 2),
        "min_execution_time_ms": round(min_time, 2),
        "max_execution_time_ms": round(max_time, 2),
        "iterations": iterations,
        "row_count": row_count,
        "status": "SUCCESS",
        "error": None
    }


def benchmark_cassandra_query_multiple(session, query_name, query_text, iterations=10):
    """
    Wykonuje zapytanie Cassandra wielokrotnie i zwraca sredni czas.
    """
    print(f"   Cassandra: {query_name} ({iterations}x)...", end=" ")

    times = []
    row_count = None

    for i in range(iterations):
        try:
            start_time = time.time()
            result = session.execute(query_text)
            rows = list(result)
            end_time = time.time()

            execution_time = (end_time - start_time) * 1000  # ms
            times.append(execution_time)
            row_count = len(rows)

        except Exception as e:
            print(f"ERROR on iteration {i+1}: {str(e)[:50]}")
            return {
                "database": "Cassandra",
                "query_type": "SELECT_WARMED_UP",
                "query_name": query_name,
                "avg_execution_time_ms": None,
                "min_execution_time_ms": None,
                "max_execution_time_ms": None,
                "iterations": iterations,
                "row_count": None,
                "status": "ERROR",
                "error": str(e)[:200]
            }

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"OK avg={avg_time:.2f}ms (min={min_time:.2f}, max={max_time:.2f}) ({row_count} rows)")

    return {
        "database": "Cassandra",
        "query_type": "SELECT_WARMED_UP",
        "query_name": query_name,
        "avg_execution_time_ms": round(avg_time, 2),
        "min_execution_time_ms": round(min_time, 2),
        "max_execution_time_ms": round(max_time, 2),
        "iterations": iterations,
        "row_count": row_count,
        "status": "SUCCESS",
        "error": None
    }


# ==========================================
# GENEROWANIE DANYCH FAKER DLA INSERT
# ==========================================

def generate_customer_data(count):
    """
    Generuje dane klientow za pomoca Faker.
    Kolumny: NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY

    Returns:
        list: Lista slownikow z danymi klientow
    """
    customers = []
    for _ in range(count):
        customers.append({
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number()[:50],
            'address': fake.street_address()[:255],
            'city': fake.city()[:100],
            'country': fake.country()[:100]
        })
    return customers


def generate_invoice_data(count, customer_id_start=1):
    """
    Generuje dane faktur za pomoca Faker.
    Kolumny: INVOICE_NUMBER, CUSTOMER_ID, ISSUE_DATE, DUE_DATE, TOTAL_AMOUNT, STATUS

    Returns:
        list: Lista slownikow z danymi faktur
    """
    invoices = []
    for i in range(count):
        issue_date = fake.date_between(start_date='-2y', end_date='today')
        due_date = fake.date_between(start_date=issue_date, end_date='+60d')
        invoices.append({
            'invoice_number': f"INV-BENCH-{fake.unique.random_number(digits=10)}",
            'customer_id': customer_id_start + (i % 1000),  # Rozklad na istniejacych klientow
            'issue_date': issue_date.strftime('%Y-%m-%d'),
            'due_date': due_date.strftime('%Y-%m-%d'),
            'total_amount': round(fake.pyfloat(min_value=10, max_value=10000, right_digits=2), 2),
            'status': fake.random_element(['PAID', 'UNPAID', 'OVERDUE'])
        })
    return invoices


# ==========================================
# FUNKCJE INSERT BENCHMARK
# ==========================================

def benchmark_sql_insert_batch(session, table_name, data, db_name):
    """
    Wykonuje batch INSERT dla SQL (Firebird/MariaDB).
    """
    count = len(data)
    print(f"   {db_name}: INSERT {table_name} ({count} rows)...", end=" ", flush=True)

    try:
        start_time = time.time()

        if table_name == 'CUSTOMER':
            for row in data:
                # Escape single quotes in string values
                name = row['name'].replace("'", "''")
                email = row['email'].replace("'", "''")
                phone = row['phone'].replace("'", "''")
                address = row['address'].replace("'", "''")
                city = row['city'].replace("'", "''")
                country = row['country'].replace("'", "''")

                query = f"""
                    INSERT INTO CUSTOMER (NAME, EMAIL, PHONE, ADDRESS, CITY, COUNTRY)
                    VALUES ('{name}', '{email}', '{phone}', '{address}', '{city}', '{country}')
                """
                session.execute(text(query))

        elif table_name == 'INVOICE':
            for row in data:
                invoice_number = row['invoice_number'].replace("'", "''")
                status = row['status'].replace("'", "''")
                query = f"""
                    INSERT INTO INVOICE (INVOICE_NUMBER, CUSTOMER_ID, ISSUE_DATE, DUE_DATE, TOTAL_AMOUNT, STATUS)
                    VALUES ('{invoice_number}', {row['customer_id']}, '{row['issue_date']}', '{row['due_date']}', {row['total_amount']}, '{status}')
                """
                session.execute(text(query))

        session.commit()
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms ({execution_time/count:.2f}ms/row)")

        return {
            "database": db_name,
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": round(execution_time, 2),
            "row_count": count,
            "time_per_row_ms": round(execution_time / count, 4),
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        session.rollback()
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": db_name,
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": None,
            "row_count": count,
            "time_per_row_ms": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_orient_insert_batch(client, table_name, data):
    """
    Wykonuje batch INSERT dla OrientDB.
    OrientDB uzywa INSERT INTO ... CONTENT {...} lub INSERT INTO ... SET ...
    """
    count = len(data)
    print(f"   OrientDB: INSERT {table_name} ({count} rows)...", end=" ", flush=True)

    try:
        start_time = time.time()

        if table_name == 'CUSTOMER':
            for row in data:
                # Escape single quotes for OrientDB
                name = row['name'].replace("'", "\\'")
                email = row['email'].replace("'", "\\'")
                phone = row['phone'].replace("'", "\\'")
                address = row['address'].replace("'", "\\'")
                city = row['city'].replace("'", "\\'")
                country = row['country'].replace("'", "\\'")

                query = f"""
                    INSERT INTO CUSTOMER SET NAME = '{name}', EMAIL = '{email}',
                    PHONE = '{phone}', ADDRESS = '{address}', CITY = '{city}', COUNTRY = '{country}'
                """
                execute_orient_command(client, query)

        elif table_name == 'INVOICE':
            for row in data:
                invoice_number = row['invoice_number'].replace("'", "\\'")
                status = row['status'].replace("'", "\\'")
                query = f"""
                    INSERT INTO INVOICE SET INVOICE_NUMBER = '{invoice_number}', CUSTOMER_ID = {row['customer_id']},
                    ISSUE_DATE = '{row['issue_date']}', DUE_DATE = '{row['due_date']}',
                    TOTAL_AMOUNT = {row['total_amount']}, STATUS = '{status}'
                """
                execute_orient_command(client, query)

        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms ({execution_time/count:.2f}ms/row)")

        return {
            "database": "OrientDB",
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": round(execution_time, 2),
            "row_count": count,
            "time_per_row_ms": round(execution_time / count, 4),
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "OrientDB",
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": None,
            "row_count": count,
            "time_per_row_ms": None,
            "status": "ERROR",
            "error": str(e)[:200]
        }


def benchmark_cassandra_insert_batch(session, table_name, data):
    """
    Wykonuje batch INSERT dla Cassandra.
    Uwaga: Cassandra ma zdenormalizowana strukture tabel.
    - Dla CUSTOMER: wstawiamy do users_by_role (jako test insert)
    - Dla INVOICE: wstawiamy do all_invoices_with_details
    """
    count = len(data)
    print(f"   Cassandra: INSERT {table_name} ({count} rows)...", end=" ", flush=True)

    try:
        start_time = time.time()

        if table_name == 'CUSTOMER':
            # Cassandra nie ma prostej tabeli CUSTOMER
            # Wstawiamy do users_by_role jako test wydajnosci INSERT
            for i, row in enumerate(data):
                name = row['name'].replace("'", "''")
                email = row['email'].replace("'", "''")
                # Generujemy unikalny user_id
                user_id = 100000 + i
                query = f"""
                    INSERT INTO users_by_role (role, user_id, username, name, email)
                    VALUES ('BENCHMARK', {user_id}, 'bench_user_{user_id}', '{name}', '{email}')
                """
                session.execute(query)

        elif table_name == 'INVOICE':
            # Wstawiamy do all_invoices_with_details
            for i, row in enumerate(data):
                invoice_number = row['invoice_number'].replace("'", "''")
                status = row['status'].replace("'", "''")
                year = int(row['issue_date'][:4])
                invoice_id = 100000 + i  # Unikalny ID dla benchmarku

                query = f"""
                    INSERT INTO all_invoices_with_details
                    (year, invoice_id, invoice_number, total_amount, status, issue_date, due_date, customer_id)
                    VALUES ({year}, {invoice_id}, '{invoice_number}', {row['total_amount']}, '{status}',
                            '{row['issue_date']}', '{row['due_date']}', {row['customer_id']})
                """
                session.execute(query)

        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # ms

        print(f"OK {execution_time:.2f}ms ({execution_time/count:.2f}ms/row)")

        return {
            "database": "Cassandra",
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": round(execution_time, 2),
            "row_count": count,
            "time_per_row_ms": round(execution_time / count, 4),
            "status": "SUCCESS",
            "error": None
        }

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {
            "database": "Cassandra",
            "query_type": "INSERT",
            "query_name": f"insert_{table_name.lower()}_{count}",
            "execution_time_ms": None,
            "row_count": count,
            "time_per_row_ms": None,
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


def run_warmed_up_select_benchmarks(fb_session, maria_session, orient_client, cassandra_session):
    """
    Uruchamia benchmarki dla zapytan SELECT na rozgrzanych bazach danych.
    Kazde zapytanie jest wykonywane 10 razy, wynik to sredni czas.
    """
    print("\n" + "="*60)
    print("BENCHMARK: SELECT QUERIES (WARMED UP - 10x avg)")
    print("="*60)

    # Pobierz przykładowe wartości do parametryzowanych zapytań
    sample_values = get_sample_values(fb_session, cassandra_session)

    # Rozgrzej bazy danych
    warm_up_database(fb_session, maria_session, orient_client, cassandra_session, sample_values)

    # Firebird SELECT (10x)
    print("\n[Firebird] SELECT (warmed up):")
    for name, query in FB_SELECT.items():
        result = benchmark_sql_query_multiple(fb_session, name, query, "Firebird", iterations=10)
        warmed_up_results.append(result)

    # MariaDB SELECT (10x)
    print("\n[MariaDB] SELECT (warmed up):")
    for name, query in MARIA_SELECT.items():
        result = benchmark_sql_query_multiple(maria_session, name, query, "MariaDB", iterations=10)
        warmed_up_results.append(result)

    # OrientDB SELECT (10x)
    print("\n[OrientDB] SELECT (warmed up):")
    for name, query in ORIENT_SELECT_QUERIES.items():
        result = benchmark_orient_query_multiple(orient_client, name, query, iterations=10)
        warmed_up_results.append(result)

    # Cassandra SELECT (10x) - z podstawionymi parametrami
    print("\n[Cassandra] SELECT (warmed up):")
    for name, query_template in CQL_SELECT_QUERIES.items():
        try:
            query = query_template.format(**sample_values)
            result = benchmark_cassandra_query_multiple(cassandra_session, name, query, iterations=10)
            warmed_up_results.append(result)
        except KeyError as e:
            print(f"   [WARN] Pominieto {name} - brak parametru {e}")
        except Exception as e:
            print(f"   [WARN] Blad w {name}: {e}")


def run_insert_benchmarks(fb_session, maria_session, orient_client, cassandra_session):
    """
    Uruchamia benchmarki dla zapytan INSERT.
    Testuje wstawianie 100, 1000 i 50000 rekordow do tabel CUSTOMER i INVOICE.
    """
    print("\n" + "="*60)
    print("BENCHMARK: INSERT QUERIES")
    print("="*60)

    # Rozmiary testów
    batch_sizes = [100, 1000, 50000]

    for batch_idx, batch_size in enumerate(batch_sizes):
        print(f"\n{'='*40}")
        print(f"INSERT TEST: {batch_size} records")
        print(f"{'='*40}")

        # Reset Faker unique generator to avoid duplicates between batches
        fake.unique.clear()

        # Generuj dane
        print(f"\n[DATA] Generowanie {batch_size} rekordow CUSTOMER...")
        customer_data = generate_customer_data(batch_size)
        print(f"[DATA] Generowanie {batch_size} rekordow INVOICE...")
        invoice_data = generate_invoice_data(batch_size)

        # === CUSTOMER INSERTS ===
        print(f"\n--- INSERT CUSTOMER ({batch_size} rows) ---")

        # Firebird
        result = benchmark_sql_insert_batch(fb_session, 'CUSTOMER', customer_data, "Firebird")
        insert_benchmark_results.append(result)

        # MariaDB
        result = benchmark_sql_insert_batch(maria_session, 'CUSTOMER', customer_data, "MariaDB")
        insert_benchmark_results.append(result)

        # OrientDB
        result = benchmark_orient_insert_batch(orient_client, 'CUSTOMER', customer_data)
        insert_benchmark_results.append(result)

        # Cassandra
        result = benchmark_cassandra_insert_batch(cassandra_session, 'CUSTOMER', customer_data)
        insert_benchmark_results.append(result)

        # === INVOICE INSERTS ===
        print(f"\n--- INSERT INVOICE ({batch_size} rows) ---")

        # Firebird
        result = benchmark_sql_insert_batch(fb_session, 'INVOICE', invoice_data, "Firebird")
        insert_benchmark_results.append(result)

        # MariaDB
        result = benchmark_sql_insert_batch(maria_session, 'INVOICE', invoice_data, "MariaDB")
        insert_benchmark_results.append(result)

        # OrientDB
        result = benchmark_orient_insert_batch(orient_client, 'INVOICE', invoice_data)
        insert_benchmark_results.append(result)

        # Cassandra
        result = benchmark_cassandra_insert_batch(cassandra_session, 'INVOICE', invoice_data)
        insert_benchmark_results.append(result)


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


def save_warmed_up_results_to_csv():
    """
    Zapisuje wyniki benchmarku warmed-up SELECT do pliku CSV.

    Returns:
        str: Ścieżka do zapisanego pliku CSV
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}_warmed_up.csv"

    print(f"\n[SAVE] Zapisywanie wynikow warmed-up do: {filename}")

    fieldnames = [
        "database",
        "query_type",
        "query_name",
        "avg_execution_time_ms",
        "min_execution_time_ms",
        "max_execution_time_ms",
        "iterations",
        "row_count",
        "status",
        "error"
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(warmed_up_results)

    print(f"OK Zapisano {len(warmed_up_results)} wynikow do {filename}")
    return filename


def save_insert_results_to_csv():
    """
    Zapisuje wyniki benchmarku INSERT do pliku CSV.

    Returns:
        str: Ścieżka do zapisanego pliku CSV
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_results_{timestamp}_insert.csv"

    print(f"\n[SAVE] Zapisywanie wynikow INSERT do: {filename}")

    fieldnames = [
        "database",
        "query_type",
        "query_name",
        "execution_time_ms",
        "row_count",
        "time_per_row_ms",
        "status",
        "error"
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(insert_benchmark_results)

    print(f"OK Zapisano {len(insert_benchmark_results)} wynikow do {filename}")
    return filename


def print_summary():
    """Wyswietla podsumowanie wynikow benchmarku"""
    print("\n" + "="*60)
    print("PODSUMOWANIE BENCHMARKU")
    print("="*60)

    # Standard benchmark results
    if benchmark_results:
        print("\n--- Standard Benchmarks ---")
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

    # Warmed-up benchmark results
    if warmed_up_results:
        print("\n--- Warmed-up SELECT Benchmarks (10x avg) ---")
        databases = set(r["database"] for r in warmed_up_results)

        for db in databases:
            db_results = [r for r in warmed_up_results if r["database"] == db]
            success_count = len([r for r in db_results if r["status"] == "SUCCESS"])
            error_count = len([r for r in db_results if r["status"] == "ERROR"])

            avg_time = None
            if success_count > 0:
                times = [r["avg_execution_time_ms"] for r in db_results if r["avg_execution_time_ms"] is not None]
                if times:
                    avg_time = sum(times) / len(times)

            print(f"\n{db}:")
            print(f"   Sukces: {success_count}")
            print(f"   Bledy: {error_count}")
            if avg_time:
                print(f"   Sredni czas (avg z 10 iteracji): {avg_time:.2f}ms")

    # Insert benchmark results
    if insert_benchmark_results:
        print("\n--- INSERT Benchmarks ---")
        databases = set(r["database"] for r in insert_benchmark_results)

        for db in databases:
            db_results = [r for r in insert_benchmark_results if r["database"] == db]
            success_count = len([r for r in db_results if r["status"] == "SUCCESS"])
            error_count = len([r for r in db_results if r["status"] == "ERROR"])

            total_rows = sum(r["row_count"] for r in db_results if r["row_count"] is not None)
            total_time = sum(r["execution_time_ms"] for r in db_results if r["execution_time_ms"] is not None)

            print(f"\n{db}:")
            print(f"   Sukces: {success_count}")
            print(f"   Bledy: {error_count}")
            print(f"   Laczna liczba wierszy: {total_rows}")
            if total_time > 0:
                print(f"   Laczny czas INSERT: {total_time:.2f}ms")


# ==========================================
# MAIN
# ==========================================

def run_benchmark(fb_session, maria_session, orient_client, run_warmed_up=True, run_inserts=True):
    """
    Główna funkcja benchmarku - przyjmuje sesje z zewnątrz.

    Args:
        fb_session: Sesja Firebird
        maria_session: Sesja MariaDB
        orient_client: Klient OrientDB
        run_warmed_up: Czy uruchomic benchmarki warmed-up SELECT (domyslnie True)
        run_inserts: Czy uruchomic benchmarki INSERT (domyslnie True)

    Returns:
        dict: Słownik ze ścieżkami do plików CSV z wynikami
    """
    print("\n" + "="*60)
    print("BENCHMARK BAZ DANYCH - START")
    print("="*60)

    # Połączenie tylko z Cassandrą (inne sesje są z parametrów)
    cassandra_session, cassandra_cluster = connect_cassandra()

    csv_paths = {}

    try:
        # Uruchomienie standardowych benchmarków
        with ExecutionTimer("SELECT benchmarks"):
            run_select_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        with ExecutionTimer("DDL benchmarks"):
            run_ddl_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        with ExecutionTimer("DML benchmarks"):
            run_dml_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        # Uruchomienie benchmarkow warmed-up SELECT
        if run_warmed_up:
            with ExecutionTimer("Warmed-up SELECT benchmarks"):
                run_warmed_up_select_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        # Uruchomienie benchmarkow INSERT
        if run_inserts:
            with ExecutionTimer("INSERT benchmarks"):
                run_insert_benchmarks(fb_session, maria_session, orient_client, cassandra_session)

        # Podsumowanie i zapis
        print_summary()

        csv_paths['standard'] = save_results_to_csv()

        if run_warmed_up and warmed_up_results:
            csv_paths['warmed_up'] = save_warmed_up_results_to_csv()

        if run_inserts and insert_benchmark_results:
            csv_paths['insert'] = save_insert_results_to_csv()

        print("\n" + "="*60)
        print("BENCHMARK BAZ DANYCH - KONIEC")
        print("="*60)

        return csv_paths

    except Exception as e:
        print(f"\n[ERROR] KRYTYCZNY BLAD: {e}")
        import traceback
        traceback.print_exc()
        return csv_paths

    finally:
        # Zamkniecie tylko Cassandry (inne sesje sa zarzadzane w main.py)
        print("\n[Cassandra] Zamykanie polaczenia...")
        cassandra_session.shutdown()
        cassandra_cluster.shutdown()


if __name__ == "__main__":
    print("[ERROR] Ten skrypt powinien byc wywolywany z main.py, nie bezposrednio!")
    print("Uzyj: python main.py")
