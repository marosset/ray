#ifdef _WIN32
#include <Windows.h>
#else
#include <chrono>
#include <thread>
#endif
#include <cstdlib>
#include <string>
#include <iostream>

// Cross-platform deterministic long-running helper process used in tests.
// Sleeps in 100ms chunks for N milliseconds (default 30000).
// Usage: sleep_loop [--millis=30000]
int main(int argc, char* argv[]) {
  int total = 30000; // 30s default
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--millis=",0)==0) {
      try {
        total = std::stoi(a.substr(9));
      } catch (...) {
        std::cerr << "Invalid --millis value; using default 30000" << std::endl;
      }
    }
  }
  if (total < 0) total = 0;
  int slept = 0;
  while (slept < total) {
#ifdef _WIN32
    Sleep(100);
#else
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
#endif
    slept += 100;
  }
  return 0;
}
