#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

std::atomic<bool> graceful_requested{false};

enum class ChildMode {
  kNone,
  kWait,
  kLeak,
  kExit,
};

struct Options {
  int exit_code = 0;
  int graceful_exit_code = 0;
  int sleep_ms = 0;
  bool handle_graceful = false;
  bool ignore_graceful = false;
  bool child_mode = false;
  ChildMode spawn_child = ChildMode::kNone;
  std::string ready_file;
  std::string marker_file;
  std::string child_marker_file;
};

bool StartsWith(const std::string &value, const std::string &prefix) {
  return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
}

int ParseIntValue(const std::string &arg, const std::string &prefix) {
  return std::stoi(arg.substr(prefix.size()));
}

std::string ParseStringValue(const std::string &arg, const std::string &prefix) {
  return arg.substr(prefix.size());
}

bool WriteFile(const std::string &path, const std::string &contents) {
  if (path.empty()) {
    return true;
  }
  std::ofstream file(path, std::ios::out | std::ios::trunc);
  if (!file.good()) {
    return false;
  }
  file << contents;
  return file.good();
}

bool AppendFile(const std::string &path, const std::string &contents) {
  if (path.empty()) {
    return true;
  }
  std::ofstream file(path, std::ios::out | std::ios::app);
  if (!file.good()) {
    return false;
  }
  file << contents;
  return file.good();
}

ChildMode ParseChildMode(const std::string &value) {
  if (value == "none") {
    return ChildMode::kNone;
  }
  if (value == "wait") {
    return ChildMode::kWait;
  }
  if (value == "leak") {
    return ChildMode::kLeak;
  }
  if (value == "exit") {
    return ChildMode::kExit;
  }
  std::cerr << "Unknown child mode: " << value << std::endl;
  std::exit(2);
}

#ifdef _WIN32
std::string QuoteArg(const std::string &arg) {
  std::string quoted = "\"";
  for (const char character : arg) {
    if (character == '"') {
      quoted += "\\\"";
    } else {
      quoted += character;
    }
  }
  quoted += "\"";
  return quoted;
}
#endif

std::vector<std::string> ChildArgs(const std::string &executable_path,
                                  const Options &options) {
  int child_sleep_ms = 0;
  if (options.spawn_child == ChildMode::kLeak) {
    child_sleep_ms = options.sleep_ms > 0 ? options.sleep_ms : 200;
  } else if (options.spawn_child == ChildMode::kWait) {
    child_sleep_ms = 20;
  }
  std::vector<std::string> args = {
      executable_path,
      "--child-mode",
      "--sleep-ms=" + std::to_string(child_sleep_ms),
      "--exit-code=0",
  };
  if (!options.child_marker_file.empty()) {
    args.push_back("--child-marker-file=" + options.child_marker_file);
  }
  return args;
}

#ifdef _WIN32
struct ChildProcess {
  PROCESS_INFORMATION process_info{};
};

std::string BuildCommandLine(const std::vector<std::string> &args) {
  std::ostringstream command_line;
  for (size_t index = 0; index < args.size(); index++) {
    if (index > 0) {
      command_line << ' ';
    }
    command_line << QuoteArg(args[index]);
  }
  return command_line.str();
}

ChildProcess StartChild(const std::vector<std::string> &args) {
  STARTUPINFOA startup_info{};
  startup_info.cb = sizeof(startup_info);
  ChildProcess child;
  std::string command_line = BuildCommandLine(args);
  std::vector<char> mutable_command_line(command_line.begin(), command_line.end());
  mutable_command_line.push_back('\0');
  if (!CreateProcessA(nullptr,
                      mutable_command_line.data(),
                      nullptr,
                      nullptr,
                      FALSE,
                      0,
                      nullptr,
                      nullptr,
                      &startup_info,
                      &child.process_info)) {
    std::cerr << "CreateProcessA failed: " << GetLastError() << std::endl;
    std::exit(5);
  }
  CloseHandle(child.process_info.hThread);
  child.process_info.hThread = nullptr;
  return child;
}

int WaitForChild(ChildProcess *child) {
  WaitForSingleObject(child->process_info.hProcess, INFINITE);
  DWORD exit_code = 0;
  GetExitCodeProcess(child->process_info.hProcess, &exit_code);
  CloseHandle(child->process_info.hProcess);
  child->process_info.hProcess = nullptr;
  return static_cast<int>(exit_code);
}

void ReleaseChild(ChildProcess *child) {
  CloseHandle(child->process_info.hProcess);
  child->process_info.hProcess = nullptr;
}
#else
struct ChildProcess {
  pid_t pid = -1;
};

ChildProcess StartChild(const std::vector<std::string> &args) {
  pid_t pid = fork();
  if (pid < 0) {
    std::cerr << "fork failed" << std::endl;
    std::exit(5);
  }
  if (pid == 0) {
    std::vector<char *> argv;
    argv.reserve(args.size() + 1);
    for (const std::string &arg : args) {
      argv.push_back(const_cast<char *>(arg.c_str()));
    }
    argv.push_back(nullptr);
    execv(args.front().c_str(), argv.data());
    _exit(127);
  }
  return ChildProcess{pid};
}

int WaitForChild(ChildProcess *child) {
  int status = 0;
  waitpid(child->pid, &status, 0);
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    return 128 + WTERMSIG(status);
  }
  return status;
}

void ReleaseChild(ChildProcess *child) { (void)child; }
#endif

bool ConfigureChildBehavior(const std::string &executable_path, const Options &options) {
  if (options.spawn_child == ChildMode::kNone) {
    return true;
  }
  ChildProcess child = StartChild(ChildArgs(executable_path, options));
  if (options.spawn_child == ChildMode::kLeak) {
    ReleaseChild(&child);
    return true;
  }
  return WaitForChild(&child) == 0;
}

#ifdef _WIN32
BOOL WINAPI ConsoleCtrlHandler(DWORD ctrl_type) {
  switch (ctrl_type) {
  case CTRL_C_EVENT:
  case CTRL_BREAK_EVENT:
  case CTRL_CLOSE_EVENT:
  case CTRL_SHUTDOWN_EVENT:
    graceful_requested.store(true, std::memory_order_release);
    return TRUE;
  default:
    return FALSE;
  }
}
#else
void SignalHandler(int signal_number) {
  (void)signal_number;
  graceful_requested.store(true, std::memory_order_release);
}
#endif

void InstallGracefulHandler() {
#ifdef _WIN32
  SetConsoleCtrlHandler(ConsoleCtrlHandler, TRUE);
#else
  signal(SIGTERM, SignalHandler);
  signal(SIGINT, SignalHandler);
#endif
}

Options ParseOptions(int argc, char **argv) {
  Options options;
  for (int index = 1; index < argc; index++) {
    std::string arg = argv[index];
    if (StartsWith(arg, "--exit-code=")) {
      options.exit_code = ParseIntValue(arg, "--exit-code=");
    } else if (StartsWith(arg, "--graceful-exit-code=")) {
      options.graceful_exit_code = ParseIntValue(arg, "--graceful-exit-code=");
    } else if (StartsWith(arg, "--sleep-ms=")) {
      options.sleep_ms = ParseIntValue(arg, "--sleep-ms=");
    } else if (StartsWith(arg, "--ready-file=")) {
      options.ready_file = ParseStringValue(arg, "--ready-file=");
    } else if (StartsWith(arg, "--marker-file=")) {
      options.marker_file = ParseStringValue(arg, "--marker-file=");
    } else if (StartsWith(arg, "--child-marker-file=")) {
      options.child_marker_file = ParseStringValue(arg, "--child-marker-file=");
    } else if (arg == "--handle-graceful") {
      options.handle_graceful = true;
    } else if (arg == "--ignore-graceful") {
      options.ignore_graceful = true;
    } else if (StartsWith(arg, "--spawn-child=")) {
      options.spawn_child = ParseChildMode(ParseStringValue(arg, "--spawn-child="));
    } else if (arg == "--child-mode") {
      options.child_mode = true;
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      std::exit(2);
    }
  }
  return options;
}

int WaitForGracefulOrTimeout(const Options &options) {
  const int sleep_ms = options.sleep_ms > 0 ? options.sleep_ms : 30000;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(sleep_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (graceful_requested.load(std::memory_order_acquire)) {
      if (options.handle_graceful) {
        if (!WriteFile(options.marker_file, "graceful\n")) {
          return 3;
        }
        return options.graceful_exit_code;
      }
      if (options.ignore_graceful) {
        graceful_requested.store(false, std::memory_order_release);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return options.exit_code;
}

int RunChild(const Options &options) {
  if (!AppendFile(options.child_marker_file, "child-started\n")) {
    return 6;
  }
  if (options.sleep_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(options.sleep_ms));
  }
  if (!AppendFile(options.child_marker_file, "child-exiting\n")) {
    return 7;
  }
  return options.exit_code;
}

}  // namespace

int main(int argc, char **argv) {
  Options options = ParseOptions(argc, argv);
  if (options.child_mode) {
    return RunChild(options);
  }
  if (options.handle_graceful || options.ignore_graceful) {
    InstallGracefulHandler();
  }
  if (!ConfigureChildBehavior(argv[0], options)) {
    return 5;
  }
  if (!WriteFile(options.ready_file, "ready\n")) {
    return 4;
  }
  if (options.handle_graceful || options.ignore_graceful || options.sleep_ms > 0) {
    return WaitForGracefulOrTimeout(options);
  }
  return options.exit_code;
}