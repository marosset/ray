#ifdef _WIN32
#include <Windows.h>
#else
#include <chrono>
#include <thread>
#endif

#include <cstdlib>
#include <iostream>
#include <string>

// Helper binary that exits with a specified code and optional delay.
// Usage: exit_with_code [--code=0] [--delay-ms=0]
int main(int argc, char *argv[]) {
  int code = 0;
  int delay_ms = 0;
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg.rfind("--code=", 0) == 0) {
      try {
        code = std::stoi(arg.substr(7));
      } catch (...) {
        std::cerr << "Invalid --code argument, defaulting to 0" << std::endl;
        code = 0;
      }
    } else if (arg.rfind("--delay-ms=", 0) == 0) {
      try {
        delay_ms = std::stoi(arg.substr(11));
      } catch (...) {
        std::cerr << "Invalid --delay-ms argument, defaulting to 0" << std::endl;
        delay_ms = 0;
      }
    }
  }

  if (delay_ms < 0) {
    delay_ms = 0;
  }

  if (delay_ms > 0) {
#ifdef _WIN32
    Sleep(static_cast<DWORD>(delay_ms));
#else
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
#endif
  }

  return code;
}
