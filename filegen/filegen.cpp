// this is a pretty slow version, but that's where my knowledge ends

#include <iostream>
#include <fstream>
#include <string>
#include <limits>
#include <random>
#include <chrono>
#include <iomanip>
#include <cstdio>
#include <cstring>

using ull = unsigned long long;

void fill_file(std::ofstream& outfile, ull filesize, std::mt19937_64& gen);

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <filename> <filesize>" << std::endl;
        return 1;
    }

    ull filesize = 0;
    try {
        filesize = std::stoull(argv[2]);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid filesize: " << argv[2] << std::endl;
        return 1;
    } catch (const std::out_of_range& e) {
        std::cerr << "Filesize out of range: " << argv[2] << std::endl;
        return 1;
    }

    std::ofstream outfile(argv[1], std::ios::binary);
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open or create file " << argv[1] << std::endl;
        return 1;
    }

    std::random_device rd;
    std::mt19937_64 gen(rd());

    auto start = std::chrono::high_resolution_clock::now();

    fill_file(outfile, filesize, gen);

    outfile.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    double duration_seconds = duration.count() / 1000.0;
    std::cout << "File generation completed in " << std::fixed << std::setprecision(5) << duration_seconds << " seconds." << std::endl;

    return 0;
}

void fill_file(std::ofstream& outfile, ull filesize, std::mt19937_64& gen) {
    constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024; // 8MB buffer
    char* buffer = new char[BUFFER_SIZE];
    size_t buf_pos = 0;

    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    constexpr int charset_size = 62;
    constexpr int max_random_str_len = 20;
    
    static const int days_in_month[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    
    ull written_bytes = 0;

    std::uniform_int_distribution<ull> ull_dist(1, std::numeric_limits<ull>::max());
    std::uniform_int_distribution<int> len_dist(1, max_random_str_len);
    std::uniform_int_distribution<int> year_dist(1900, 2025);
    std::uniform_int_distribution<int> month_dist(0, 11);
    std::uniform_int_distribution<int> char_dist(0, charset_size - 1);
    std::uniform_int_distribution<int> day_dist(1, 31);

    while (written_bytes < filesize) {
        ull arg = ull_dist(gen);
        int random_str_len = len_dist(gen);
        int year = year_dist(gen);
        int month_idx = month_dist(gen);
        int month = month_idx + 1;
        
        int max_day = days_in_month[month_idx];
        if (month_idx == 1) { // February
            bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if (is_leap) max_day = 29;
        }
        int day = 1 + (day_dist(gen) % max_day);

        char temp[128];
        int pos = sprintf(temp, "%llu-", arg);
        
        for (int i = 0; i < random_str_len; ++i) {
            temp[pos++] = charset[char_dist(gen)];
        }
        
        pos += sprintf(temp + pos, "-%04d/%02d/%02d\n", year, month, day);

        if (buf_pos + pos > BUFFER_SIZE) {
            outfile.write(buffer, buf_pos);
            buf_pos = 0;
        }

        memcpy(buffer + buf_pos, temp, pos);
        buf_pos += pos;
        written_bytes += pos;
    }

    if (buf_pos > 0) {
        outfile.write(buffer, buf_pos);
    }

    delete[] buffer;
}