import time

class ExecutionTimer:
    def __init__(self, name="Query"):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.duration = 0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Koniec pomiaru
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time


# Przykład użycia:
# with ExecutionTimer("Test SQL") as timer:
#     time.sleep(1) # Tu leci Twoje zapytanie do bazy
# print(f"Wynik: {timer.duration}")

class Benchmark:
    def __init__(self, session_fb, session_maria):
        self.session_fb = session_fb
        self.session_maria = session_maria

    def dml_insert_benchmark(self):
        pass
    def dml_update_benchmark(self):
        pass


