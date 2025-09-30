// I DON'T CLAIM OWNERSHIP OF THIS CODE. IT'S AI GENERATED. IT'S BEYOND MY KNOWLEDGE.

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <random>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <windows.h>

using ull = unsigned long long;

class PCG64 {
    ull state;
    static constexpr ull multiplier = 6364136223846793005ULL;
    static constexpr ull increment = 0xda3e39cb94b95bdbULL;
    
public:
    explicit PCG64(ull seed = 0x853c49e6748fea9bULL) : state(seed) {}
    
    inline ull operator()() {
        ull oldstate = state;
        state = oldstate * multiplier + increment;
        ull xorshifted = ((oldstate >> 18u) ^ oldstate) >> 27u;
        ull rot = oldstate >> 59u;
        return (xorshifted >> rot) | (xorshifted << ((-rot) & 63));
    }
};

class FastIntToStr {
    static const char digits_lut[200];
    
public:
    static inline int convert(ull value, char* buffer) {
        char temp[20];
        char* p = temp + 19;
        *p = '\0';

        if (value == 0) {
            buffer[0] = '0';
            return 1;
        }

        while (value >= 100) {
            auto const old = value;
            value /= 100;
            memcpy(p - 2, &digits_lut[(old - value * 100) * 2], 2);
            p -= 2;
        }

        if (value >= 10) {
            memcpy(p - 2, &digits_lut[value * 2], 2);
            p -= 2;
        } else if (value > 0) {
            *--p = '0' + value;
        }

        int len = temp + 19 - p;
        memcpy(buffer, p, len);
        return len;
    }
};

const char FastIntToStr::digits_lut[200] = {
    '0','0','0','1','0','2','0','3','0','4','0','5','0','6','0','7','0','8','0','9',
    '1','0','1','1','1','2','1','3','1','4','1','5','1','6','1','7','1','8','1','9',
    '2','0','2','1','2','2','2','3','2','4','2','5','2','6','2','7','2','8','2','9',
    '3','0','3','1','3','2','3','3','3','4','3','5','3','6','3','7','3','8','3','9',
    '4','0','4','1','4','2','4','3','4','4','4','5','4','6','4','7','4','8','4','9',
    '5','0','5','1','5','2','5','3','5','4','5','5','5','6','5','7','5','8','5','9',
    '6','0','6','1','6','2','6','3','6','4','6','5','6','6','6','7','6','8','6','9',
    '7','0','7','1','7','2','7','3','7','4','7','5','7','6','7','7','7','8','7','9',
    '8','0','8','1','8','2','8','3','8','4','8','5','8','6','8','7','8','8','8','9',
    '9','0','9','1','9','2','9','3','9','4','9','5','9','6','9','7','9','8','9','9'
};

constexpr size_t CHUNK_SIZE = 256 * 1024 * 1024;
constexpr size_t WRITE_BUFFER_SIZE = 128 * 1024 * 1024;
constexpr int BATCH_SIZE = 512;

struct ThreadData {
    char* buffer;
    size_t buffer_size;
    ull start_offset;
    ull target_size;
    ull seed;
    int thread_id;
    std::atomic<bool>* should_stop;
};

struct PrecomputedData {
    static const char charset[63];
    static constexpr int charset_size = 62;
    static const int days_in_month[12];
    
    char year_strings[126][5]{};
    char month_strings[12][3]{};
    char day_strings[32][3]{};
    
    PrecomputedData() {
        for (int i = 0; i < 126; ++i) {
            int year = 1900 + i;
            year_strings[i][0] = '0' + (year / 1000);
            year_strings[i][1] = '0' + ((year / 100) % 10);
            year_strings[i][2] = '0' + ((year / 10) % 10);
            year_strings[i][3] = '0' + (year % 10);
            year_strings[i][4] = '\0';
        }
        
        for (int i = 0; i < 12; ++i) {
            int month = i + 1;
            month_strings[i][0] = '0' + (month / 10);
            month_strings[i][1] = '0' + (month % 10);
            month_strings[i][2] = '\0';
        }
        
        for (int i = 0; i < 31; ++i) {
            int day = i + 1;
            day_strings[i][0] = '0' + (day / 10);
            day_strings[i][1] = '0' + (day % 10);
            day_strings[i][2] = '\0';
        }
    }
};

const char PrecomputedData::charset[63] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const int PrecomputedData::days_in_month[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static PrecomputedData precomputed;

DWORD WINAPI generate_worker(LPVOID param) {
    auto* td = static_cast<ThreadData *>(param);
    
    #ifdef _MSC_VER
        td->buffer = (char*)_aligned_malloc(td->buffer_size, 64);
    #else
        td->buffer = static_cast<char *>(_mm_malloc(td->buffer_size, 64));
    #endif
    
    if (!td->buffer) {
        std::cerr << "Thread " << td->thread_id << ": Failed to allocate buffer\n";
        return 1;
    }
    
    PCG64 gen(td->seed);

    char* ptr = td->buffer;
    char* buffer_end = td->buffer + td->buffer_size - 1024;
    ull bytes_generated = 0;
    
    ull numbers[BATCH_SIZE];
    unsigned char str_lens[BATCH_SIZE];
    unsigned char months[BATCH_SIZE];
    unsigned char years[BATCH_SIZE];
    unsigned char days[BATCH_SIZE];
    
    while (bytes_generated < td->target_size && !td->should_stop->load()) {
        if (ptr >= buffer_end) {
            break;
        }
        
        for (int i = 0; i < BATCH_SIZE; ++i) {
            ull rnd = gen();
            numbers[i] = gen();
            str_lens[i] = 1 + (rnd & 0x1F) % 20;
            months[i] = ((rnd >> 5) & 0xF) % 12;
            years[i] = (rnd >> 9) % 126;
            
            int max_day = PrecomputedData::days_in_month[months[i]];
            if (months[i] == 1) {
                int year = 1900 + years[i];
                if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
                    max_day = 29;
                }
            }
            days[i] = gen() % max_day;
        }
        
        for (int i = 0; i < BATCH_SIZE; ++i) {
            if (bytes_generated >= td->target_size) break;
            
            int num_len = FastIntToStr::convert(numbers[i], ptr);
            ptr += num_len;
            *ptr++ = '-';
            
            ull char_rnd = gen();
            for (int j = 0; j < str_lens[i]; ++j) {
                if (j % 10 == 0 && j > 0) char_rnd = gen();
                *ptr++ = PrecomputedData::charset[char_rnd % PrecomputedData::charset_size];
                char_rnd >>= 6;
            }
            *ptr++ = '-';
            
            memcpy(ptr, precomputed.year_strings[years[i]], 4);
            ptr += 4;
            *ptr++ = '/';
            memcpy(ptr, precomputed.month_strings[months[i]], 2);
            ptr += 2;
            *ptr++ = '/';
            memcpy(ptr, precomputed.day_strings[days[i]], 2);
            ptr += 2;
            *ptr++ = '\n';
            
            bytes_generated = ptr - td->buffer;
        }
    }
    
    td->target_size = bytes_generated;
    return 0;
}

void generate_file_parallel(const char* filename, ull filesize) {
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    unsigned int num_threads = sysinfo.dwNumberOfProcessors;
    if (num_threads == 0) num_threads = 4;
    
    if (filesize >= 10ULL * 1024 * 1024 * 1024) {
        num_threads = std::min(num_threads * 2, 32u);
    }
    
    std::cout << "Generating " << (filesize / (1024.0 * 1024.0 * 1024.0)) 
              << " GB using " << num_threads << " threads\n";
    
    HANDLE hFile = CreateFileA(
        filename,
        GENERIC_WRITE,
        0,
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_WRITE_THROUGH,
        nullptr
    );
    
    if (hFile == INVALID_HANDLE_VALUE) {
        std::cerr << "Failed to create file: " << GetLastError() << std::endl;
        return;
    }
    
    LARGE_INTEGER li;
    li.QuadPart = filesize;
    if (!SetFilePointerEx(hFile, li, nullptr, FILE_BEGIN) || !SetEndOfFile(hFile)) {
        std::cerr << "Failed to pre-allocate file space\n";
        CloseHandle(hFile);
        return;
    }
    SetFilePointer(hFile, 0, nullptr, FILE_BEGIN);
    
    std::random_device rd;
    std::atomic<bool> should_stop(false);
    
    ull total_written = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    auto last_report = start_time;
    
    while (total_written < filesize) {
        ull remaining = filesize - total_written;
        ull chunk_to_generate = std::min(remaining, (ull)CHUNK_SIZE);
        
        ull per_thread = chunk_to_generate / num_threads;
        
        std::vector<HANDLE> threads(num_threads);
        std::vector<ThreadData> thread_data(num_threads);
        
        for (unsigned int i = 0; i < num_threads; ++i) {
            thread_data[i].buffer = nullptr;
            thread_data[i].buffer_size = (i == num_threads - 1) 
                ? (chunk_to_generate - per_thread * i) 
                : per_thread;
            thread_data[i].buffer_size += 1024;
            thread_data[i].start_offset = total_written + (per_thread * i);
            thread_data[i].target_size = (i == num_threads - 1) 
                ? (chunk_to_generate - per_thread * i) 
                : per_thread;
            thread_data[i].seed = rd() ^ (static_cast<ull>(rd()) << 32) ^ (static_cast<ull>(i) << 48);
            thread_data[i].thread_id = i;
            thread_data[i].should_stop = &should_stop;
            
            threads[i] = CreateThread(nullptr, 0, generate_worker, &thread_data[i], 0, nullptr);
            if (threads[i] == nullptr) {
                std::cerr << "Failed to create thread " << i << std::endl;
                should_stop = true;
                break;
            }
        }
        
        WaitForMultipleObjects(num_threads, threads.data(), TRUE, INFINITE);
        
        DWORD bytes_written;
        for (unsigned int i = 0; i < num_threads; ++i) {
            if (thread_data[i].buffer && thread_data[i].target_size > 0) {
                if (!WriteFile(hFile, thread_data[i].buffer, 
                             (DWORD)thread_data[i].target_size, 
                             &bytes_written, nullptr)) {
                    std::cerr << "Write failed: " << GetLastError() << std::endl;
                    should_stop = true;
                }
                total_written += bytes_written;
                
                #ifdef _MSC_VER
                    _aligned_free(thread_data[i].buffer);
                #else
                    _mm_free(thread_data[i].buffer);
                #endif
            }
            CloseHandle(threads[i]);
        }
        
        auto now = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 5) {
            double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count() / 1000.0;
            double gb_written = total_written / (1024.0 * 1024.0 * 1024.0);
            double throughput = (total_written / (1024.0 * 1024.0)) / elapsed;
            double progress = (total_written * 100.0) / filesize;
            
            std::cout << std::fixed << std::setprecision(1)
                     << "Progress: " << progress << "% | "
                     << gb_written << " GB | "
                     << throughput << " MB/s | "
                     << "ETA: " << ((filesize - total_written) / (1024.0 * 1024.0)) / throughput << " seconds\n";
            
            last_report = now;
        }
        
        if (should_stop) break;
    }
    
    CloseHandle(hFile);
    
    auto end_time = std::chrono::high_resolution_clock::now();
    double total_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() / 1000.0;
    double throughput = (total_written / (1024.0 * 1024.0)) / total_seconds;
    
    std::cout << "\n=== Final Statistics ===\n";
    std::cout << "Total written: " << (total_written / (1024.0 * 1024.0 * 1024.0))
              << " GB (" << total_written << " bytes)\n";
    std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_seconds << " seconds\n";
    std::cout << "Average throughput: " << std::fixed << std::setprecision(1) << throughput << " MB/s\n";
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <filename> <filesize_in_bytes>\n";
        std::cerr << "Example: " << argv[0] << " output.txt 10737418240  (for 10GB)\n";
        std::cerr << "         " << argv[0] << " output.txt 1099511627776 (for 1TB)\n";
        return 1;
    }
    
    ull filesize = 0;
    try {
        filesize = std::stoull(argv[2]);
    } catch (const std::exception& e) {
        std::cerr << "Invalid filesize: " << argv[2] << std::endl;
        return 1;
    }
    
    if (filesize == 0) {
        std::cerr << "Filesize must be greater than 0\n";
        return 1;
    }
    
    generate_file_parallel(argv[1], filesize);
    
    return 0;
}