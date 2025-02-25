#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <map>
#include <chrono>

// Mutex per proteggere l'accesso ai dati condivisi
std::mutex mtx;

// Contatore per i byte scritti
size_t bytesWritten = 0;

void countLetters(const std::string& text, std::unordered_map<char, int>& letterCounts, size_t start, size_t end) {
    std::unordered_map<char, int> localCounts;
    for (size_t i = start; i < end; ++i) {
        char c = text[i];
        if (std::isalpha(c)) {
            c = std::tolower(c); // Convertiamo le lettere in minuscolo per semplificare il conteggio
            localCounts[c]++;
        }
    }
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto& pair : localCounts) {
        letterCounts[pair.first] += pair.second;
    }
}

void writeOutput(const std::unordered_map<char, int>& letterCounts) {
    std::lock_guard<std::mutex> lock(mtx);
    // Utilizziamo std::map per ordinare le lettere alfabeticamente
    std::map<char, int> sortedCounts(letterCounts.begin(), letterCounts.end());
    for (const auto& pair : sortedCounts) {
        std::string output = std::string(1, pair.first) + ": " + std::to_string(pair.second) + "\n";
        std::cout << output;
        bytesWritten += output.size();
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <filename> <num_threads>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    int numThreads = std::stoi(argv[2]);

    // Legge il file di testo
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "Error opening file " << filename << std::endl;
        return 1;
    }

    size_t fileSize = file.tellg();
    file.seekg(0, std::ios::beg);

    std::string text(fileSize, '\0');
    file.read(&text[0], fileSize);
    file.close();

    auto start_time = std::chrono::high_resolution_clock::now();

    std::unordered_map<char, int> letterCounts;
    std::vector<std::thread> threads;
    size_t blockSize = fileSize / numThreads;

    for (int i = 0; i < numThreads; ++i) {
        size_t start = i * blockSize;
        size_t end = (i == numThreads - 1) ? fileSize : (i + 1) * blockSize;
        threads.emplace_back(countLetters, std::ref(text), std::ref(letterCounts), start, end);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    // Stampa i risultati e conta i byte scritti
    writeOutput(letterCounts);

    std::cout << "Total bytes written: " << bytesWritten << std::endl;
    std::cout << "Total execution time: " << duration.count() << " seconds" << std::endl;

    return 0;
}