#include <cstdint>
#include <exception>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <utility>

#include "external_sort/external_sort.h"
#include "storage/file.h"

using namespace std::literals::string_view_literals;

static void usage(const char* argv0) {
  std::cerr << "Usage: " << argv0 << " [--help] generate|print|sort [<options>]"
            << std::endl;
  std::cerr << R"(
Options for generate:
    generate [--random] <count> <output_file>

    "generate" creates the file <output_file> that contains <count> integers in
    decending order, or in random order when --random is given.

Options for print:
    print <input_file>

    "print" prints all integers contained in <input_file>.

Options for sort
    sort <input_file> <output_file> <mem_size>

    "sort" sorts the integers contained in <input_file> and writes them into
    <output_file> by using buzzdb::external_sort().
)";
}

/**
 * @brief 
 *  The loop continues until all count integers are written to the file.
 *  It checks if at least 512 integers remain (i + 512 <= count):
 *    - If true, it writes a full batch of 512 integers.
 *    - If false, it writes only the remaining integers (less than 512).
 * 
 * @ref
 * std::forward (https://en.cppreference.com/w/cpp/utility/forward)
 * : A template function used for achieving perfect forwarding of arguments to functions so that it's lvalue or rvalue is preserved. It basically forwards the argument while preserving the value type of it.
 * 
 * vector::data() (https://cplusplus.com/reference/vector/vector/data/)
 * : Returns a direct pointer to the memory array used internally by the vector to store its owned elements.
 * 
 * reinterpret_cast <data_type *>(pointer_variable) (https://www.geeksforgeeks.org/reinterpret_cast-in-c-type-casting-operators/)
 * : A type of casting operator used in C++. It is used to convert a pointer of some data type into a pointer of another data type,
 * 
 * TODO 
 * - external_sort 코드 분석* 페이지 보면서 c++ 관련 노트 테이킹하기... 
 */

template <typename F>
static void write_values(buzzdb::File& file, size_t count, F&& get_value) {
  std::vector<uint64_t> values(512);
  size_t i = 0;
  while (true) {
    if (i + 512 <= count) {
      size_t last_i = i;
      for (size_t j = 0; j < 512; ++j) {
        values[j] = std::forward<F>(get_value)(i, count);
        ++i;
      }
      file.write_block(reinterpret_cast<const char*>(values.data()),
                       last_i * sizeof(uint64_t), 512 * sizeof(uint64_t));
    } else {
      size_t last_i = i;
      size_t remaining = count - i;
      for (size_t j = 0; j < remaining; ++j) {
        values[j] = std::forward<F>(get_value)(i, count);
        ++i;
      }
      file.write_block(reinterpret_cast<const char*>(values.data()),
                       last_i * sizeof(uint64_t), remaining * sizeof(uint64_t));
      break;
    }
  }
}

static int mode_generate(int argc, const char* argv[]) {
  using File = buzzdb::File;
  if (argc < 4 || argc > 5) {
    usage(argv[0]);
    return 0;
  }
  bool random;
  const char* count_str;
  const char* filename;
  if (argc == 5) {
    if (argv[2] != "--random"sv) {
      usage(argv[0]);
      return 0;
    }
    random = true;
    count_str = argv[3];
    filename = argv[4];
  } else {
    random = false;
    count_str = argv[2];
    filename = argv[3];
  }
  size_t count;
  {
    std::string count_s(count_str);
    size_t pos = 0;
    count = std::stoull(count_s, &pos);
    if (pos != count_s.size()) {
      usage(argv[0]);
      return 0;
    }
  }
  auto file = File::open_file(filename, File::WRITE);
  file->resize(count * sizeof(uint64_t));
  if (random) {
    std::mt19937_64 engine{0};
    std::uniform_int_distribution<uint64_t> distr;
    write_values(*file, count, [&](size_t, size_t) { return distr(engine); });
  } else {
    write_values(*file, count,
                 [](size_t i, size_t count) { return count - i; });
  }
  return 0;
}

int mode_print(int argc, const char* argv[]) {
  using File = buzzdb::File;
  if (argc != 3) {
    usage(argv[0]);
    return 0;
  }
  auto file = File::open_file(argv[2], File::READ);
  size_t file_size = file->size();
  std::vector<uint64_t> values(512);
  size_t offset = 0;
  while (offset + 512 * sizeof(uint64_t) <= file_size) {
    file->read_block(offset, 512 * sizeof(uint64_t),
                     reinterpret_cast<char*>(values.data()));
    for (uint64_t value : values) {
      std::cout << value << ' ';
    }
    offset += 512 * sizeof(uint64_t);
  }
  size_t remaining = (file_size - offset) / sizeof(uint64_t);
  file->read_block(offset, remaining * sizeof(uint64_t),
                   reinterpret_cast<char*>(values.data()));
  for (size_t i = 0; i < remaining; ++i) {
    std::cout << values[i] << ' ';
  }
  std::cout.flush();
  return 0;
}

int mode_sort(int argc, const char* argv[]) {
  using File = buzzdb::File;
  if (argc != 5) {
    usage(argv[0]);
    return 0;
  }
  size_t mem_size;
  {
    std::string mem_size_s(argv[4]);
    size_t pos = 0;
    mem_size = std::stoull(mem_size_s, &pos);
    if (pos != mem_size_s.size()) {
      usage(argv[0]);
      return 0;
    }
  }
  auto input_file = File::open_file(argv[2], File::READ);
  auto output_file = File::open_file(argv[3], File::WRITE);
  buzzdb::external_sort(*input_file, input_file->size() / sizeof(uint64_t),
                           *output_file, mem_size);
  return 0;
}

int main(int argc, const char* argv[]) {
  if (argc <= 2) {
    usage(argv[0]);
    return 0;
  }
  std::string_view mode{argv[1]};
  try {
    if (mode == "generate"sv) {
      return mode_generate(argc, argv);
    } else if (mode == "print"sv) {
      return mode_print(argc, argv);
    } else if (mode == "sort"sv) {
      return mode_sort(argc, argv);
    } else {
      usage(argv[0]);
      return 0;
    }
  } catch (std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
