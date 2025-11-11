#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <cstdint>

using namespace std;

const size_t MEMORY_LIMIT = 500 * 1024 * 1024; // 500 MB
const size_t RECORD_SIZE = 100;
const size_t CHUNK_SIZE = MEMORY_LIMIT / RECORD_SIZE;
const int K = 3;

struct Record {
    uint64_t key;
    string line;

    Record() : key(0) {};
    Record(uint64_t k, const string& l) : key(k), line(l) {}
};

uint64_t extract_key(const string& line) {
    size_t pos = line.find('-');
    if (pos != string::npos) {
        return stoull(line.substr(0, pos));
    }
    return 0;
}

int count_series(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) return 0;

    int series_count = 0;
    uint64_t prev_key = UINT64_MAX;
    string line;
    bool in_series = false;

    while (getline(file, line)) {
        if (line.empty()) continue;

        uint64_t key = extract_key(line);

        if (!in_series || key > prev_key) {
            series_count++;
            in_series = true;
        }

        prev_key = key;
    }

    file.close();
    return series_count;
}

void initial_distribution(const string& input_file, vector<string>& b_files) {
    ifstream input(input_file);
    vector<ofstream> outputs(K);

    b_files.clear();
    for (int i = 0; i < K; i++) {
        b_files.push_back("B" + to_string(i + 1) + ".tmp");
        outputs[i].open(b_files[i]);
    }

    vector<Record> buffer;
    string line;
    int current_file = 0;
    int series_count = 0;

    while (getline(input, line)) {
        if (line.empty()) continue;

        uint64_t key = extract_key(line);
        buffer.push_back(Record(key, line));

        if (buffer.size() >= CHUNK_SIZE) {
            sort(buffer.begin(), buffer.end(), [](const Record& a, const Record& b) { return a.key > b.key; });

            for (const auto& rec : buffer) {
                outputs[current_file] << rec.line << "\n";
            }

            current_file = (current_file + 1) % K;
            series_count++;
            buffer.clear();
        }
    }

    if (!buffer.empty()) {
        sort(buffer.begin(), buffer.end(), [](const Record& a, const Record& b) { return a.key > b.key; });

        for (const auto& rec : buffer) {
            outputs[current_file] << rec.line << "\n";
        }
        series_count++;
    }

    for (auto& out : outputs) {
        out.close();
    }

    input.close();
    cout << "Initial distribution: " << series_count << " series distributed to " << K << " B files\n";
}

struct Reader {
    ifstream* file;
    Record current;
    uint64_t prev_key;
    bool has_record;
    bool series_ended;

    Reader() : file(nullptr), prev_key(UINT64_MAX), has_record(false), series_ended(true) {}

    void init(ifstream* file) {
        this->file = file;
        prev_key = UINT64_MAX;
        has_record = false;
        series_ended = true;
    }

    bool start_new_series() {
        prev_key = UINT64_MAX;
        series_ended = false;
        return read_next();
    }

    bool read_next() {
        if (!file || !file->is_open() || series_ended) return false;

        string line;
        if (getline(*file, line)) {
            if (line.empty()) return read_next();

            current.key = extract_key(line);
            current.line = line;

            if (current.key > prev_key) {
                series_ended = true;
                file->seekg(-line.length() - 1, ios::cur);
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

void merge(vector<string>& input_files, vector<string>& output_files) {
    vector<ifstream> inputs(K);
    vector<ofstream> outputs(K);
    vector<Reader> readers(K);

    for (int i = 0; i < K; i++) {
        inputs[i].open(input_files[i]);
        readers[i].init(&inputs[i]);
    }

    output_files.clear();
    for (int i = 0; i < K; i++) {
        output_files.push_back("C" + to_string(i + 1) + ".tmp");
        outputs[i].open(output_files[i]);
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

        priority_queue<pair<uint64_t, int>, vector<pair<uint64_t, int>>, greater<pair<uint64_t, int>>> pq;

        for (int i = 0; i < K; i++) {
            if (readers[i].has_record) {
                pq.push(make_pair(readers[i].current.key, i));
            }
        }

        while (!pq.empty()) {
            pair<uint64_t, int> top = pq.top();
            pq.pop();

            uint64_t key = top.first;
            int idx = top.second;

            outputs[current_output] << readers[idx].current.line << "\n";

            if (readers[idx].read_next()) {
                pq.push(make_pair(readers[idx].current.key, idx));
            }
        }

        total_series_merged++;
        current_output = (current_output + 1) % K;
    }

    for (auto& in : inputs) in.close();
    for (auto& out : outputs) out.close();

    cout << " Merged" << total_series_merged << " series, distributed to " << K << " C files\n";
}

int check_completion(const vector<string>& files) {
    int file_with_data = -1;
    int total_files_with_data = 0;

    for (int i = 0; i < K; i++) {
        ifstream file(files[i]);
        file.seekg(0, ios::end);
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

void merge_sort(const string& input_file, const string& output_file) {
    vector<string> b_files, c_files;

    cout << "--- Initial distribution ---\n";
    initial_distribution(input_file, b_files);

    int pass = 0;
    bool merging_from_b = true;

    cout << "--- Merge phase ---\n";
    while (true) {
        pass++;
        cout << "Pass " << pass << ":\n";

        if (merging_from_b) {
            int b_result = check_completion(b_files);
            if (b_result == -2) {
                cerr << "Error: All B files are empty before merge\n";
                return;
            }
            if (b_result >= 0) {
                rename(c_files[b_result].c_str(), output_file.c_str());
                for (const auto& file : b_files) remove(file.c_str());
                for (int i = 0; i < K; i++) {
                    if (i != b_result) remove(c_files[i].c_str());
                }
                break;
            }

            cout << "B1, B2, ... , B" << K << " -> C1, C2, ... , C" << K << "\n";
            merge(b_files, c_files);

            int result = check_completion(c_files);
            if (result == -2) {
                cerr << "Error: All C files are empty after merge!\n";
                return;
            }
            if (result >= 0) {
                rename(c_files[result].c_str(), output_file.c_str());
                for (const auto& file : b_files) remove(file.c_str());
                for (int i = 0; i < K; i++) {
                    if (i != result) remove(c_files[i].c_str());
                }
                break;
            }

            merging_from_b = false;

        } else {
            int c_result = check_completion(c_files);
            if (c_result == -2) {
                cerr << "Error: All C files are empty after merge!\n";
                return;
            }
            if (c_result >= 0) {
                rename(c_files[c_result].c_str(), output_file.c_str());
                for (const auto& file : c_files) remove(file.c_str());
                for (int i = 0; i < K; i++) {
                    if (i != c_result) remove(c_files[i].c_str());
                }
                break;
            }

            cout << "C1, C2, ... , C" << K << " -> B1, B2, ... , B" << K << "\n";
            merge(c_files, b_files);

            int result = check_completion(b_files);
            if (result == -2) {
                cerr << "Error: All B files are empty after merge!\n";
                return;
            }
            if (result >= 0) {
                rename(b_files[result].c_str(), output_file.c_str());
                for (const auto& file : c_files) remove(file.c_str());
                for (int i = 0; i < K; i++) {
                    if (i != result) remove(b_files[i].c_str());
                }
                break;
            }

            merging_from_b = true;
        }
    }

    cout << "\nComplexity: O(log_" << K << " n) passes, O(n log_" << K << " n) copies\n";
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <input_file> <output_file>\n";
        return 1;
    }

    string input_file = argv[1];
    string output_file = argv[2];

    merge_sort(input_file, output_file);

    cout << "\nSort complete! Output: " << output_file << "\n";

    return 0;
}