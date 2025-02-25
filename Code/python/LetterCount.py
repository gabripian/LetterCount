import time
from collections import defaultdict, OrderedDict
import sys

# Classe che implementa il Mapper con In-Mapper Combining
class LetterCountMapper:
    def __init__(self):
        # Inizializza un dizionario per contare le occorrenze delle lettere
        self.letter_counts = defaultdict(int)

    def setup(self):
        # Metodo chiamato all'inizio della fase di mapping per inizializzare o resettare i contatori
        self.letter_counts.clear()

    def map(self, key, value):
        # Metodo che prende una chiave (indice di riga) e un valore (linea di testo)
        # e conta le occorrenze delle lettere
        for char in value.lower():
            if 'a' <= char <= 'z':  # Considera solo le lettere dell'alfabeto inglese
                self.letter_counts[char] += 1

    def cleanup(self):
        # Metodo chiamato alla fine della fase di mapping per restituire il conteggio delle lettere
        return self.letter_counts

# Classe che implementa il Reducer
class LetterCountReducer:
    def reduce(self, key, values):
        # Metodo che prende una chiave e una lista di dizionari di conteggi e li combina
        total_count = defaultdict(int)
        for count_dict in values:
            for letter, count in count_dict.items():
                total_count[letter] += count
        return total_count

# Funzione principale per eseguire il processo MapReduce
def run_map_reduce(input_file):
    start_time = time.time()  # Inizia a misurare il tempo di esecuzione
    
    # Simula gli InputSplits leggendo le righe del file di input
    input_splits = []
    with open(input_file, 'r') as f:
        input_splits = f.readlines()
    
    # Inizializza il Mapper e il Reducer
    mapper = LetterCountMapper()
    reducer = LetterCountReducer()
    
    # Esegue il Mapper
    intermediate_results = []
    mapper.setup()  # Chiamata al metodo setup del Mapper
    for idx, line in enumerate(input_splits):
        mapper.map(idx, line.strip())  # Chiamata al metodo map per ogni linea di testo
    intermediate_results.append(mapper.cleanup())  # Chiamata al metodo cleanup del Mapper
    
    # Esegue il Reducer
    final_result = reducer.reduce(None, intermediate_results)  # Combina i risultati intermedi
    
    end_time = time.time()  # Termina la misurazione del tempo di esecuzione
    execution_time = end_time - start_time  # Calcola il tempo totale di esecuzione
    num_input_splits = len(input_splits)  # Conta il numero di InputSplits (linee di testo)
    
    # Ordina il dizionario per lettere
    sorted_result = OrderedDict(sorted(final_result.items()))
    
    return sorted_result, execution_time, num_input_splits  # Restituisce i risultati finali

# Esegue lo script se viene chiamato direttamente
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]  # Percorso al file di testo di input
    letter_counts, execution_time, num_input_splits = run_map_reduce(input_file)  # Esegue il MapReduce
    
    # Stampa i risultati
    print(f"Letter Counts: {letter_counts}")
    print(f"Execution Time: {execution_time} seconds")
    print(f"Number of InputSplits: {num_input_splits}")
