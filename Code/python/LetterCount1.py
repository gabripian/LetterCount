import threading
import time
import os

# Funzione che conta le occorrenze delle lettere in un dato intervallo di testo
def count_letters(text, result_dict):
    letter_count = {}
    for char in text:
        if char.isalpha():
            char_lower = char.lower()  # Considera lettere minuscole
            if char_lower in letter_count:
                letter_count[char_lower] += 1
            else:
                letter_count[char_lower] = 1
    
    # Aggiunge il risultato parziale al dizionario condiviso
    for char, count in letter_count.items():
        if char in result_dict:
            result_dict[char] += count
        else:
            result_dict[char] = count

# Funzione per leggere il file e distribuire il lavoro ai thread
def process_file(filename, num_threads):
    start_time = time.time()
    
    # Legge il contenuto del file
    with open(filename, 'r') as file:
        text = file.read()
    
    # Dizionario condiviso per contenere il risultato
    result_dict = {}
    
    # Calcola la lunghezza approssimativa dei chunk per i thread
    chunk_size = len(text) // num_threads
    threads = []
    
    # Crea e avvia i thread
    for i in range(num_threads):
        start = i * chunk_size
        # Ultimo thread gestisce eventuali caratteri rimanenti
        end = start + chunk_size if i < num_threads - 1 else len(text)
        thread = threading.Thread(target=count_letters, args=(text[start:end], result_dict))
        threads.append(thread)
        thread.start()
    
    # Aspetta che tutti i thread abbiano completato
    for thread in threads:
        thread.join()
    
    # Ordina il risultato in ordine alfabetico delle chiavi
    sorted_result = sorted(result_dict.items())
    
    # Calcola il numero totale di byte scritti durante l'esecuzione
    total_bytes_written = sum(len(char.encode('utf-8')) + len(str(count).encode('utf-8')) + 2 for char, count in sorted_result)
    
    # Calcola il tempo di esecuzione totale
    end_time = time.time()
    total_time = end_time - start_time
    
    # Stampa il risultato
    print("Occorrenze delle lettere in ordine alfabetico:")
    for char, count in sorted_result:
        print(f"{char}: {count}")
    
    print(f"Numero di byte scritti durante l'esecuzione: {total_bytes_written} bytes")
    print(f"Tempo di esecuzione totale: {total_time:.4f} secondi")

# Esempio di utilizzo
if __name__ == "__main__":
    filename = input("Inserisci il nome del file di testo: ")
    num_threads = int(input("Inserisci il numero di thread da utilizzare: "))
    process_file(filename, num_threads)