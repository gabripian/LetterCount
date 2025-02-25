#include <iostream>
#include <fstream>
#include <unordered_map>
#include <map>
#include <vector>
#include <chrono>
#include <cctype>
#include <string>
#include <sstream>

// Classe che implementa il Mapper con In-Mapper Combining
class LetterCountMapper {
public:
    // Metodo chiamato all'inizio della fase di mapping per inizializzare o resettare i contatori
    void setup() {
        letter_counts.clear();
    }

    // Funzione che prende una chiave (indice di riga) e un valore (linea di testo)
    // e conta le occorrenze delle lettere
    void map(int key, const std::string &value) {
        for (char ch : value) {
            ch = std::tolower(ch); // Converti il carattere in minuscolo
            if (ch >= 'a' && ch <= 'z') { // Considera solo le lettere dell'alfabeto inglese
                ++letter_counts[ch];
            }
        }
    }

    // Metodo chiamato alla fine della fase di mapping per restituire il conteggio delle lettere
    std::unordered_map<char, int> cleanup() {
        return letter_counts;
    }

private:
    std::unordered_map<char, int> letter_counts;
};

// Classe che implementa il Reducer
class LetterCountReducer {
public:
    // Metodo che prende una chiave e una lista di dizionari di conteggi e li combina
    std::unordered_map<char, int> reduce(const std::vector<std::unordered_map<char, int>> &values) {
        std::unordered_map<char, int> total_count;
        for (const auto &count_dict : values) {
            for (const auto &pair : count_dict) {
                total_count[pair.first] += pair.second;
            }
        }
        return total_count;
    }
};

// Funzione principale per eseguire il processo MapReduce
std::tuple<std::map<char, int>, double, int> run_map_reduce(const std::string &input_file) {
    auto start_time = std::chrono::high_resolution_clock::now(); // Inizia a misurare il tempo di esecuzione
    
    // Simula gli InputSplits leggendo le righe del file di input
    std::vector<std::string> input_splits;
    std::ifstream infile(input_file);
    std::string line;
    while (std::getline(infile, line)) {
        input_splits.push_back(line);
    }
    
    // Inizializza il Mapper e il Reducer
    LetterCountMapper mapper;
    LetterCountReducer reducer;
    
    // Esegue il Mapper
    mapper.setup(); // Chiamata al metodo setup del Mapper
    std::vector<std::unordered_map<char, int>> intermediate_results;
    for (int idx = 0; idx < input_splits.size(); ++idx) {
        mapper.map(idx, input_splits[idx]); // Chiamata al metodo map per ogni linea di testo
    }
    intermediate_results.push_back(mapper.cleanup()); // Chiamata al metodo cleanup del Mapper
    
    // Esegue il Reducer
    std::unordered_map<char, int> final_result = reducer.reduce(intermediate_results); // Combina i risultati intermedi
    
    auto end_time = std::chrono::high_resolution_clock::now(); // Termina la misurazione del tempo di esecuzione
    std::chrono::duration<double> execution_time = end_time - start_time; // Calcola il tempo totale di esecuzione
    int num_input_splits = input_splits.size(); // Conta il numero di InputSplits (linee di testo)
    
    // Ordina il dizionario per lettere
    std::map<char, int> sorted_result(final_result.begin(), final_result.end());
    
    return {sorted_result, execution_time.count(), num_input_splits}; // Restituisce i risultati finali
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file>" << std::endl;
        return 1;
    }
    
    std::string input_file = argv[1];
    auto [letter_counts, execution_time, num_input_splits] = run_map_reduce(input_file); // Esegue il MapReduce
    
    // Stampa i risultati
    std::cout << "Letter Counts:" << std::endl;
    for (const auto &pair : letter_counts) {
        std::cout << pair.first << ": " << pair.second << std::endl;
    }
    std::cout << "Execution Time: " << execution_time << " seconds" << std::endl;
    std::cout << "Number of InputSplits: " << num_input_splits << std::endl;

    return 0;
}
