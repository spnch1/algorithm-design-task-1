#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <cstdint>
#include <chrono>
#include <iomanip>
#include <limits>

const size_t MEMORY_LIMIT = 500 * 1024 * 1024; // 500 MB

struct Record {
    uint64_t key;
    std::string line;

    Record() : key(0) {};
    Record(uint64_t k, const std::string& l) : key(k), line(l) {}
};

uint64_t extract_key(const std::string& line) {
    size_t pos = line.find('-');

    if (pos != std::string::npos) {
        std::string num = line.substr(0, pos);
        size_t start = num.find_first_not_of(" \t\r\n");
        size_t end = num.find_last_not_of(" \t\r\n");

        if (start == std::string::npos) return 0;

        num = num.substr(start, end - start + 1);

        try {
            return stoull(num);
        } catch (...) {
            return 0;
        }
    }
    return 0;
}

void initial_distribution_standard(const std::string& input_file, std::vector<std::string>& b_files, int K) {
    std::ifstream input(input_file, std::ios::binary);
    std::vector<std::ofstream> outputs(K);

    b_files.clear();
    for (int i = 0; i < K; i++) {
        b_files.push_back("B" + std::to_string(i + 1) + ".tmp");
        outputs[i].open(b_files[i], std::ios::binary);
    }

    int current_file = 0;
    int series_count = 0;
    std::string line;
    uint64_t prev_key = UINT64_MAX;
    bool in_series = false;

    while (std::getline(input, line)) {
        if (line.empty()) continue;

        uint64_t key = extract_key(line);

        if (in_series && key > prev_key) {
            current_file = (current_file + 1) % K;
            series_count++;
            in_series = false;
        }

        outputs[current_file] << line << "\n";
        prev_key = key;
        in_series = true;
    }

    if (in_series) {
        series_count++;
    }

    for (auto& out : outputs) {
        out.close();
    }
    input.close();
    
    std::cout << "Initial distribution (standard): " << series_count << " natural series found and distributed to " << K << " B files\n";
}

void initial_distribution_optimized(const std::string& input_file, std::vector<std::string>& b_files, int K) {
    std::ifstream input(input_file, std::ios::binary);
    std::vector<std::ofstream> outputs(K);

    b_files.clear();
    for (int i = 0; i < K; i++) {
        b_files.push_back("B" + std::to_string(i + 1) + ".tmp");
        outputs[i].open(b_files[i], std::ios::binary);
    }

    std::vector<Record> buffer;
    std::string line;
    int current_file = 0;
    int series_count = 0;
    size_t current_buffer_bytes = 0;

    while (getline(input, line)) {
        if (line.empty()) continue;

        uint64_t key = extract_key(line);
        buffer.push_back(Record(key, line));
        current_buffer_bytes += line.length();

        if (current_buffer_bytes >= MEMORY_LIMIT) {
            sort(buffer.begin(), buffer.end(), [](const Record& a, const Record& b) { 
                return a.key > b.key; 
            });

            for (const auto& rec : buffer) {
                outputs[current_file] << rec.line << "\n";
            }

            current_file = (current_file + 1) % K;
            series_count++;
            buffer.clear();
            current_buffer_bytes = 0;
        }
    }

    if (!buffer.empty()) {
        sort(buffer.begin(), buffer.end(), [](const Record& a, const Record& b) { 
            return a.key > b.key; 
        });

        for (const auto& rec : buffer) {
            outputs[current_file] << rec.line << "\n";
        }
        series_count++;
    }

    for (auto& out : outputs) {
        out.close();
    }
    input.close();
    
    std::cout << "Initial distribution (optimized): " << series_count << " large sorted series created (500MB each) and distributed to " << K << " B files\n";
}

struct Reader {
    std::ifstream* file;
    Record current;
    uint64_t prev_key;
    bool has_record;
    bool series_ended;
    std::streampos last_pos;

    Reader() : file(nullptr), prev_key(UINT64_MAX), has_record(false), series_ended(true), last_pos(0) {}

    void init(std::ifstream* file) {
        this->file = file;
        prev_key = UINT64_MAX;
        has_record = false;
        series_ended = true;
        last_pos = 0;
    }

    bool start_new_series() {
        prev_key = UINT64_MAX;
        series_ended = false;
        has_record = false;
        return read_next();
    }

    bool read_next() {
        if (!file || !file->is_open() || series_ended) return false;

        std::string line;
        
        last_pos = file->tellg(); 
        
        if (getline(*file, line)) {
            if (line.empty()) return read_next();

            current.key = extract_key(line);
            current.line = line;

            if (has_record && current.key > prev_key) {
                file->seekg(last_pos); 

                series_ended = true;
                return false;
            }

            prev_key = current.key;
            has_record = true;
            return true;
        }

        series_ended = true;
        return false;
    }
};

void merge(std::vector<std::string>& input_files, std::vector<std::string>& output_files, int K) {
    std::vector<std::ifstream> inputs(K);
    std::vector<std::ofstream> outputs(K);
    std::vector<Reader> readers(K);

    for (int i = 0; i < K; i++) {
        inputs[i].open(input_files[i], std::ios::binary);
        readers[i].init(&inputs[i]);
    }

    for (int i = 0; i < K; i++) {
        outputs[i].open(output_files[i], std::ios::binary);
    }

    int current_output = 0;
    int total_series_merged = 0;

    while (true) {
        int active_sources = 0;
        for (int i = 0; i < K; i++) {
            if (readers[i].start_new_series()) {
                active_sources++;
            }
        }

        if (active_sources == 0) break;

        std::priority_queue<std::pair<uint64_t, int>> pq;

        for (int i = 0; i < K; i++) {
            if (readers[i].has_record) {
                pq.push(std::make_pair(readers[i].current.key, i));
            }
        }

        while (!pq.empty()) {
            std::pair<uint64_t, int> top = pq.top();
            pq.pop();

            int idx = top.second;

            outputs[current_output] << readers[idx].current.line << "\n";

            if (readers[idx].read_next()) {
                pq.push(std::make_pair(readers[idx].current.key, idx));
            }
        }

        total_series_merged++;
        current_output = (current_output + 1) % K;
    }

    for (auto& in : inputs) in.close();
    for (auto& out : outputs) out.close();

    std::cout << "  Merged " << total_series_merged << " series, distributed to " << K << " output files\n";
}

int check_completion(const std::vector<std::string>& files, int K) {
    int file_with_data = -1;
    int total_files_with_data = 0;

    for (int i = 0; i < K; i++) {
        std::ifstream file(files[i], std::ios::binary);
        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.close();

        if (size > 0) {
            total_files_with_data++;
            file_with_data = i;
        }
    }

    if (total_files_with_data == 1) {
        return file_with_data;
    }

    if (total_files_with_data == 0) {
        return -2;
    }

    return -1;
}

void merge_sort(const std::string& input_file, const std::string& output_file, bool use_standard, int K) {
    std::vector<std::string> b_files, c_files;

    std::cout << "--- Initial distribution ---\n";
    
    if (use_standard) {
        initial_distribution_standard(input_file, b_files, K);
    } else {
        initial_distribution_optimized(input_file, b_files, K);
    }

    for (int i = 0; i < K; i++) {
        c_files.push_back("C" + std::to_string(i + 1) + ".tmp");
    }

    std::vector<std::string> *input_names = &b_files;
    std::vector<std::string> *output_names = &c_files;

    int pass = 0;
    std::cout << "--- Merge phase ---\n";

    while (true) {
        pass++;
        std::cout << "Pass " << pass << ":\n";

        int result = check_completion(*input_names, K);

        if (result == -2) {
            std::cerr << "Error: All input files are empty before merge\n";
            return;
        }

        if (result >= 0) {
            std::string final_file = (*input_names)[result];
            rename(final_file.c_str(), output_file.c_str());

            for (const auto& file : *input_names) {
                if (file != final_file) remove(file.c_str());
            }
            for (const auto& file : *output_names) {
                remove(file.c_str());
            }
            break;
        }

        std::cout << "  Merging...\n";
        merge(*input_names, *output_names, K);

        std::swap(input_names, output_names);
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3 || argc > 4) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file> [--std]\n";
        std::cerr << "\nModes:\n";
        std::cerr << "  (default): OPTIMIZED - Pre-sort 500MB chunks in RAM, creates large sorted series\n";
        std::cerr << "  --std:     STANDARD - Read natural series from input file as-is\n";
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    bool use_standard = (argc == 4 && std::string(argv[3]) == "--std");

    int K = 0;
    while (K < 2 || K > 10) {
        std::cout << "Enter the number of merge ways (K) [2-10]: ";
        if (!(std::cin >> K)) {
            std::cin.clear();
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            std::cerr << "Invalid input. Please enter a number.\n";
        } else if (K < 2 || K > 10) {
            std::cerr << "Value must be between 2 and 10.\n";
        }
    }
    std::cout << "Using K = " << K << "\n";

    auto start_time = std::chrono::high_resolution_clock::now();

    merge_sort(input_file, output_file, use_standard, K);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\nSort complete! Output: " << output_file << "\n";
    std::cout << "Total time: " << std::fixed << std::setprecision(3) 
              << duration.count() / 1000.0 << " seconds\n";

    return 0;
}