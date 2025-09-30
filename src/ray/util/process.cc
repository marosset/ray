// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/util/process.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <Windows.h>
#include <Winternl.h>
#include <process.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <thread>

#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/subreaper.h"

#ifdef __APPLE__
extern char **environ;

// macOS doesn't come with execvpe.
// https://stackoverflow.com/questions/7789750/execve-with-path-search
int execvpe(const char *program, char *const argv[], char *const envp[]) {
  char **saved = environ;
  int rc;
  // Mutating environ is generally unsafe, but this logic only runs on the
  // start of a worker process. There should be no concurrent access to the
  // environment.
  environ = const_cast<char **>(envp);
  rc = execvp(program, argv);
  environ = saved;
  return rc;
}
#endif

namespace ray {

#if !defined(_WIN32)
void SetFdCloseOnExec(int fd) {
  if (fd < 0) {
    return;
  }
  int flags = fcntl(fd, F_GETFD, 0);
  RAY_CHECK_NE(flags, -1) << "fcntl error: errno = " << errno << ", fd = " << fd;
  const int ret = fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
  RAY_CHECK_NE(ret, -1) << "fcntl error: errno = " << errno << ", fd = " << fd;
  RAY_LOG(DEBUG) << "set FD_CLOEXEC to fd " << fd;
}
#endif

bool EnvironmentVariableLess::operator()(char a, char b) const {
  // TODO(mehrdadn): This is only used on Windows due to current lack of Unicode support.
  // It should be changed when Process adds Unicode support on Windows.
  return std::less<char>()(tolower(a), tolower(b));
}

bool EnvironmentVariableLess::operator()(const std::string &a,
                                         const std::string &b) const {
  bool result;
#ifdef _WIN32
  result = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(), *this);
#else
  result = a < b;
#endif
  return result;
}

class ProcessFD {
  pid_t pid_;
  intptr_t fd_;

 public:
  ~ProcessFD();
  ProcessFD();
  explicit ProcessFD(pid_t pid, intptr_t fd = -1);
  ProcessFD(ProcessFD &&other);
  ProcessFD &operator=(ProcessFD &&other);

  ProcessFD(const ProcessFD &other) = delete;
  ProcessFD &operator=(const ProcessFD &other) = delete;

  void CloseFD();
  intptr_t GetFD() const;
  pid_t GetId() const;

  // Fork + exec combo. Returns -1 for the PID on failure.
  static ProcessFD spawnvpe(const char *argv[],
                            std::error_code &ec,
                            bool decouple,
                            const ProcessEnvironment &env,
                            bool pipe_to_stdin) {
    ec = std::error_code();
    intptr_t fd;
    pid_t pid;
    ProcessEnvironment new_env;
    for (char *const *e = environ; *e; ++e) {
      RAY_CHECK(*e && **e != '\0') << "environment variable name is absent";
      const char *key_end = strchr(*e + 1 /* +1 is needed for Windows */, '=');
      RAY_CHECK(key_end) << "environment variable value is absent: " << e;
      new_env[std::string(*e, static_cast<size_t>(key_end - *e))] = key_end + 1;
    }
    for (const auto &item : env) {
      new_env[item.first] = item.second;
    }
    std::string new_env_block;
    for (const auto &item : new_env) {
      new_env_block += item.first + '=' + item.second + '\0';
    }
#ifdef _WIN32

    (void)decouple;  // Windows doesn't require anything particular for decoupling.
    std::vector<std::string> args;
    for (size_t i = 0; argv[i]; ++i) {
      args.push_back(argv[i]);
    }
    std::string cmds[] = {std::string(), CreateCommandLine(args)};
    if (GetFileName(args.at(0)).find('.') == std::string::npos) {
      // Some executables might be missing an extension.
      // Append a single "." to prevent automatic appending of extensions by the system.
      std::vector<std::string> args_direct_call = args;
      args_direct_call[0] += ".";
      cmds[0] = CreateCommandLine(args_direct_call);
    }
    bool succeeded = false;
    PROCESS_INFORMATION pi = {};
    for (int attempt = 0; attempt < static_cast<int>(sizeof(cmds) / sizeof(*cmds));
         ++attempt) {
      std::string &cmd = cmds[attempt];
      if (cmd.empty()) {
        continue;
      }
      // Ensure mutable, null-terminated buffer (C++17 data() gives non-const char*).
      char *cmdline = cmd.data();
      STARTUPINFOA si;
      memset(&si, 0, sizeof(si));
      si.cb = sizeof(si);
      RAY_UNUSED(new_env_block.c_str());  // ensure final null terminator
      char *envp = new_env_block.empty() ? nullptr : new_env_block.data();
      if (CreateProcessA(NULL, cmdline, NULL, NULL, FALSE, 0, envp, NULL, &si, &pi)) {
        succeeded = true;
        break;
      }
    }
    if (succeeded) {
      CloseHandle(pi.hThread);
      fd = reinterpret_cast<intptr_t>(pi.hProcess);
      pid = pi.dwProcessId;
    } else {
      ec = std::error_code(GetLastError(), std::system_category());
      fd = -1;
      pid = -1;
    }
#else
    std::vector<char *> new_env_ptrs;
    for (size_t i = 0; i < new_env_block.size(); i += strlen(&new_env_block[i]) + 1) {
      new_env_ptrs.push_back(&new_env_block[i]);
    }
    new_env_ptrs.push_back(static_cast<char *>(NULL));
    char **envp = &new_env_ptrs[0];

    // TODO(mehrdadn): Use clone() on Linux or posix_spawnp() on Mac to avoid duplicating
    // file descriptors into the child process, as that can be problematic.
    // pipefds: only created when decoupling to (1) retrieve grandchild PID and
    // (2) hold a read end whose closure signals process lifetime when we cannot waitpid.
    int pipefds[2] = {-1, -1};
    int parent_lifetime_pipe[2];

    if (decouple) {
      // TODO: On modern Linux (>=5.3) replace this lifetime pipe +
      // double-fork approach with pidfd_open() (and possibly pidfd_send_signal()) so
      // we can (1) avoid the second fork, (2) poll() or epoll() directly on the pidfd,
      // and (3) obtain the real exit status of decoupled processes instead of defaulting
      // to 0. This will unify behavior with Windows where we always get the exit code.
      if (pipe(pipefds) == -1) {
        pipefds[0] = pipefds[1] = -1;
      }
    }
    // parent_lifetime_pipe is used for child to check parent's health.
    if (pipe_to_stdin) {
      if (pipe(parent_lifetime_pipe) == -1) {
        parent_lifetime_pipe[0] = parent_lifetime_pipe[1] = -1;
      }
    }

    pid = fork();

    // Close unneeded ends of decouple pipe in each process.
    if (decouple) {
      if (pid <= 0 && pipefds[0] != -1) {
        // Child (or failure path) closes read end.
        close(pipefds[0]);
        pipefds[0] = -1;
      }
      if (pid != 0 && pipefds[1] != -1) {
        // Parent closes write end and marks read end CLOEXEC.
        close(pipefds[1]);
        pipefds[1] = -1;
        SetFdCloseOnExec(pipefds[0]);
      }
    }

    // Create a pipe and redirect the read pipe to a child's stdin.
    // Child can use it to detect the parent's lifetime.
    // See the below link for details.
    // https://stackoverflow.com/questions/12193581/detect-death-of-parent-process
    if (pipe_to_stdin) {
      if (pid <= 0 && parent_lifetime_pipe[1] != -1) {
        // Child. Close sthe write end of the pipe from child.
        close(parent_lifetime_pipe[1]);
        parent_lifetime_pipe[1] = -1;
        SetFdCloseOnExec(parent_lifetime_pipe[0]);
      }
      if (pid != 0 && parent_lifetime_pipe[0] != -1) {
        // Parent. Close the read end of the pipe.
        close(parent_lifetime_pipe[0]);
        parent_lifetime_pipe[0] = -1;
        // Make sure the write end of the pipe is closed on exec.
        SetFdCloseOnExec(parent_lifetime_pipe[1]);
      }
    } else {
      // parent_lifetime_pipe pipes are not used.
      parent_lifetime_pipe[0] = -1;
      parent_lifetime_pipe[1] = -1;
    }

  if (pid == 0) {
      // Child process case. Reset the SIGCHLD handler.
      signal(SIGCHLD, SIG_DFL);
      // If process needs to be decoupled, double-fork to avoid zombies.
      if (pid_t pid2 = decouple ? fork() : 0) {
        _exit(pid2 == -1 ? errno : 0);  // Parent of grandchild; must exit
      }

      // Redirect the read pipe to stdin so that child can track the
      // parent lifetime.
      if (parent_lifetime_pipe[0] != -1) {
        dup2(parent_lifetime_pipe[0], STDIN_FILENO);
      }

      // This is the spawned process (or grandchild if decoupled). Any intermediate parent is now dead.
      pid_t my_pid = getpid();
      if (!decouple || (pipefds[1] != -1 &&
                        write(pipefds[1], &my_pid, sizeof(my_pid)) == sizeof(my_pid))) {
        execvpe(argv[0], const_cast<char *const *>(argv), const_cast<char *const *>(envp));
      }
      _exit(errno);  // fork() succeeded and exec() failed, so abort the child
    }
    if (pid > 0) {
      // Parent process case
      if (decouple) {
        int s;
        (void)waitpid(pid, &s, 0);  // can't do much if this fails, so ignore return value
        int r = read(pipefds[0], &pid, sizeof(pid));
        (void)r;  // can't do much if this fails, so ignore return value
      }
    }
    // Only use lifetime pipe when decoupled; otherwise allow waitpid to get exit code.
    fd = decouple ? pipefds[0] : -1;
    if (pid == -1) {
      ec = std::error_code(errno, std::system_category());
    }
#endif
    return ProcessFD(pid, fd);
  }
};

ProcessFD::~ProcessFD() { CloseFD(); }

ProcessFD::ProcessFD() : pid_(-1), fd_(-1) {}

ProcessFD::ProcessFD(pid_t pid, intptr_t fd) : pid_(pid), fd_(fd) {
  if (pid != -1) {
    bool process_does_not_exist = false;
    std::error_code error;
#ifdef _WIN32
    if (fd == -1) {
      BOOL inheritable = FALSE;
      DWORD permissions = MAXIMUM_ALLOWED;
      HANDLE handle = OpenProcess(permissions, inheritable, static_cast<DWORD>(pid));
      if (handle) {
        fd_ = reinterpret_cast<intptr_t>(handle);
      } else {
        DWORD error_code = GetLastError();
        error = std::error_code(error_code, std::system_category());
        if (error_code == ERROR_INVALID_PARAMETER) {
          process_does_not_exist = true;
        }
      }
    } else {
      RAY_CHECK(pid == GetProcessId(reinterpret_cast<HANDLE>(fd)));
    }
#else
    if (kill(pid, 0) == -1 && errno == ESRCH) {
      process_does_not_exist = true;
    }
#endif
    // Don't verify anything if the PID is too high, since that's used for testing
    if (pid < PID_MAX_LIMIT) {
      if (process_does_not_exist) {
        // NOTE: This indicates a race condition where a process died and its process
        // table entry was removed before the ProcessFD could be instantiated. For
        // processes owned by this process, we should make this impossible by keeping
        // the SIGCHLD signal. For processes not owned by this process, we need to come up
        // with a strategy to create this class in a way that avoids race conditions.
        RAY_LOG(ERROR) << "Process " << pid << " does not exist.";
      }
      if (error) {
        // TODO(mehrdadn): Should this be fatal, or perhaps returned as an error code?
        // Failures might occur due to reasons such as permission issues.
        RAY_LOG(ERROR) << "error " << error << " opening process " << pid << ": "
                       << error.message();
      }
    }
  }
}

ProcessFD::ProcessFD(ProcessFD &&other) : ProcessFD() { *this = std::move(other); }

ProcessFD &ProcessFD::operator=(ProcessFD &&other) {
  if (this != &other) {
    // We use swap() to make sure the argument is actually moved from
    using std::swap;
    swap(pid_, other.pid_);
    swap(fd_, other.fd_);
  }
  return *this;
}

void ProcessFD::CloseFD() {
  if (fd_ != -1) {
    bool success;
#ifdef _WIN32
    success = !!CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    success = close(static_cast<int>(fd_)) == 0;
#endif
    RAY_CHECK(success) << "error " << errno << " closing process " << pid_ << " FD";
  }

  fd_ = -1;
}

intptr_t ProcessFD::GetFD() const { return fd_; }

pid_t ProcessFD::GetId() const { return pid_; }

Process::~Process() {}
// Default constructed (null) process conceptually represents an already-invalid
// handle; treat it as having a terminal exit code of -1 so async queries can be
// satisfied immediately without spawning waiter threads.
// NOTE: exit_code_ atomic default initializes to kStillRunning; we override here.
// (We can't assign directly in the member declaration because we want the special
// sentinel for spawned processes but immediate readiness for null/dummy.)
// This relies on the compiler generating code after the atomic's default ctor.
Process::Process() : exit_code_promise_(nullptr) { exit_code_.store(-1, std::memory_order_release); }

Process::Process(const Process &other)
    : p_(other.p_),
      exit_code_(other.exit_code_.load()),
      exit_code_promise_(other.exit_code_promise_) {}

Process::Process(Process &&other) noexcept
    : p_(std::move(other.p_)),
      exit_code_(other.exit_code_.load()),
      exit_code_promise_(std::move(other.exit_code_promise_)) {}

Process &Process::operator=(Process other) {
  p_ = std::move(other.p_);
  exit_code_ = other.exit_code_.load();
  exit_code_promise_ = std::move(other.exit_code_promise_);
  return *this;
}

Process::Process(pid_t pid) : exit_code_promise_(nullptr) {
  p_ = std::make_shared<ProcessFD>(pid);
  if (pid < 0) {
    // Dummy process (placeholder) — treat as already successfully exited (0).
    exit_code_.store(0, std::memory_order_release);
  }
}

Process::Process(const char *argv[],
                 void *io_service,
                 std::error_code &ec,
                 bool decouple,
                 const ProcessEnvironment &env,
                 bool pipe_to_stdin)
    : exit_code_promise_(nullptr) {
  /// TODO: use io_service with boost asio notify_fork.
  (void)io_service;
#ifdef __linux__
  KnownChildrenTracker::instance().AddKnownChild([&, this]() -> pid_t {
    ProcessFD procfd = ProcessFD::spawnvpe(argv, ec, decouple, env, pipe_to_stdin);
    if (!ec) {
      this->p_ = std::make_shared<ProcessFD>(std::move(procfd));
    }
    return this->GetId();
  });
#else
  ProcessFD procfd = ProcessFD::spawnvpe(argv, ec, decouple, env, pipe_to_stdin);
  if (!ec) {
    p_ = std::make_shared<ProcessFD>(std::move(procfd));
  }
#endif
}

std::error_code Process::Call(const std::vector<std::string> &args,
                              const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code ec;
  Process proc(&*argv.begin(), NULL, ec, true, env);
  if (!ec) {
    int return_code = proc.Wait();
    if (return_code != 0) {
      ec = std::error_code(return_code, std::system_category());
    }
  }
  return ec;
}

std::string Process::Exec(const std::string command) {
  /// Based on answer in
  /// https://stackoverflow.com/questions/478898/how-do-i-execute-a-command-and-get-the-output-of-the-command-within-c-using-po
  std::array<char, 128> buffer;
  std::string result;
#ifdef _WIN32
  std::unique_ptr<FILE, decltype(&_pclose)> pipe(_popen(command.c_str(), "r"), _pclose);
#else
  std::unique_ptr<FILE, int (*)(FILE *)> pipe(popen(command.c_str(), "r"), pclose);
#endif
  RAY_CHECK(pipe != nullptr) << "popen() failed for command: " << command;
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

Process Process::CreateNewDummy() {
  pid_t pid = -1;
  Process result(pid);
  return result;
}

Process Process::FromPid(pid_t pid) {
  RAY_DCHECK(pid >= 0);
  Process result(pid);
  return result;
}

const void *Process::Get() const { return p_ ? &*p_ : NULL; }

pid_t Process::GetId() const { return p_ ? p_->GetId() : -1; }

bool Process::IsNull() const { return !p_; }

bool Process::IsValid() const { return GetId() != -1; }

int Process::ExitCode() const { return exit_code_.load(std::memory_order_acquire); }

std::shared_future<int> Process::WaitAsync() {
  // Fast path: previously created future.
  {
    std::lock_guard<std::mutex> lk(wait_async_mu_);
    if (exit_code_future_.valid()) {
      return exit_code_future_;
    }
  }

  int cached = exit_code_.load(std::memory_order_acquire);
  if (cached != kStillRunning) {
    // Process already finished (Wait() or Kill() path) but no async waiter was ever
    // requested. Materialize a ready future now.
    std::promise<int> ready_promise;
    ready_promise.set_value(cached);
    auto fut = ready_promise.get_future().share();
    std::lock_guard<std::mutex> lk(wait_async_mu_);
    if (!exit_code_future_.valid()) {
      exit_code_future_ = fut;
    }
    return exit_code_future_;
  }

  // Need to initialize async wait state exactly once.
  std::shared_ptr<std::promise<int>> new_promise;
  bool spawn_waiter = false;
  {
    std::lock_guard<std::mutex> lk(wait_async_mu_);
    if (!exit_code_future_.valid()) {
      new_promise = std::make_shared<std::promise<int>>();
      exit_code_future_ = new_promise->get_future().share();
      exit_code_promise_ = new_promise;  // consumed by Wait() completion path.
      spawn_waiter = true;
    }
  }

  if (spawn_waiter) {
    // Launch detached waiter thread to perform blocking wait and fulfill promise.
    // Assumption: caller retains Process object lifetime until future becomes ready.
    std::thread([this]() {
      int code = this->DoWait();  // Direct OS wait, bypasses future check.
      // If DoWait() executed before promise installed (race), attempt to publish now.
      if (exit_code_promise_) {
        try {
          // set_value may throw on double set; ignore.
          exit_code_promise_->set_value(code);
        } catch (...) {
        }
      }
    }).detach();
  }

  return exit_code_future_;
}

std::pair<Process, std::error_code> Process::Spawn(const std::vector<std::string> &args,
                                                   bool decouple,
                                                   const std::string &pid_file,
                                                   const ProcessEnvironment &env) {
  std::vector<const char *> argv;
  argv.reserve(args.size() + 1);
  for (size_t i = 0; i != args.size(); ++i) {
    argv.push_back(args[i].c_str());
  }
  argv.push_back(NULL);
  std::error_code error;
  Process proc(&*argv.begin(), NULL, error, decouple, env);
  if (!error && !pid_file.empty()) {
    std::ofstream file(pid_file, std::ios_base::out | std::ios_base::trunc);
    file << proc.GetId() << std::endl;
    RAY_CHECK(file.good());
  }
  return std::make_pair(std::move(proc), error);
}

int Process::DoWait() const {
  // Internal wait implementation that directly waits on OS primitives.
  // This is called by both Wait() and the WaitAsync() detached thread.
  int result = -1;
  if (!p_) {
    result = -1;  // null process
  } else {
    pid_t pid = p_->GetId();
    if (pid < 0) {
      result = 0;  // dummy process
    } else {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      if (handle == NULL) {
        // No waitable process handle. This happens if the Process was created via
        // FromPid() and OpenProcess() failed (e.g. process already exited or
        // insufficient rights). Treat as unknown (-1) without logging a noisy
        // ERROR_INVALID_HANDLE.
        result = -1;
      } else {
        DWORD exit_code = STILL_ACTIVE;
        if (WaitForSingleObject(handle, INFINITE) == WAIT_OBJECT_0 &&
            GetExitCodeProcess(handle, &exit_code)) {
          result = static_cast<int>(exit_code);
        } else {
          error = std::error_code(GetLastError(), std::system_category());
          result = -1;
        }
      }
#else
      if (fd != -1) {
        // Wait on pipe closure to detect death (grandchild tracking). Exit code unknown.
        unsigned char buf[256];
        ptrdiff_t r;
        while ((r = read(fd, buf, sizeof(buf))) > 0) {
        }
        result = (r == -1) ? -1 : 0;
      } else {
        int status = 0;
        if (waitpid(pid, &status, 0) == -1) {
          error = std::error_code(errno, std::system_category());
          result = -1;
        } else if (WIFEXITED(status)) {
          result = WEXITSTATUS(status);
        } else if (WIFSIGNALED(status)) {
          result = 128 + WTERMSIG(status);
        } else {
          result = -1;
        }
      }
#endif
      if (error) {
        RAY_LOG(ERROR) << "Failed to wait for process " << pid << " with error " << error
                       << ": " << error.message();
      }
    }
  }

  // Attempt to publish result (single-writer expected). If another thread beat us,
  // prefer the published value.
  int expected = kStillRunning;
  if (!exit_code_.compare_exchange_strong(expected, result, std::memory_order_acq_rel)) {
    result = expected;  // someone else set it; use that value
  } else if (exit_code_promise_) {
    try {
      exit_code_promise_->set_value(result);
    } catch (...) {
      // Ignore double-set or broken promise.
    }
  }
  return result;
}

int Process::Wait() const {
  int cached = exit_code_.load(std::memory_order_acquire);
  if (cached != kStillRunning) {
    return cached;  // already waited
  }

  // Check if WaitAsync() has already been called and a waiter thread is active.
  // If so, wait on the future instead of trying to wait on the OS handle directly,
  // which would fail on Windows (handle can only be waited on once).
  std::shared_future<int> async_future;
  {
    std::lock_guard<std::mutex> lk(wait_async_mu_);
    if (exit_code_future_.valid()) {
      async_future = exit_code_future_;
    }
  }
  if (async_future.valid()) {
    // WaitAsync() waiter thread is handling the OS wait; piggyback on its future.
    async_future.wait();
    return async_future.get();
  }

  // No async wait in progress; perform synchronous wait directly.
  return DoWait();
}

bool Process::IsAlive() const {
  if (p_) {
    return IsProcessAlive(p_->GetId());
  }
  return false;
}

void Process::Kill() {
  if (p_) {
    pid_t pid = p_->GetId();
    if (pid >= 0) {
      std::error_code error;
      intptr_t fd = p_->GetFD();
#ifdef _WIN32
      HANDLE handle = fd != -1 ? reinterpret_cast<HANDLE>(fd) : NULL;
      if (!::TerminateProcess(handle, ERROR_PROCESS_ABORTED)) {
        error = std::error_code(GetLastError(), std::system_category());
        if (error.value() == ERROR_ACCESS_DENIED) {
          // This can occur in some situations if the process is already terminating.
          DWORD exit_code;
          if (GetExitCodeProcess(handle, &exit_code) && exit_code != STILL_ACTIVE) {
            // The process is already terminating, so consider the killing successful.
            error = std::error_code();
          }
        }
      }
#else
      pollfd pfd = {static_cast<int>(fd), POLLHUP};
      if (fd != -1 && poll(&pfd, 1, 0) == 1 && (pfd.revents & POLLHUP)) {
        // The process has already died; don't attempt to kill its PID again.
      } else if (kill(pid, SIGKILL) != 0) {
        error = std::error_code(errno, std::system_category());
      }
      if (error.value() == ESRCH) {
        // The process died before our kill().
        // This is probably due to using FromPid().Kill() on a non-owned process.
        // We got lucky here, because we could've killed a recycled PID.
        // To avoid this, do not kill a process that is not owned by us.
        // Instead, let its parent receive its SIGCHLD normally and call waitpid() on it.
        // (Exception: Tests might occasionally trigger this, but that should be benign.)
      }
#endif
      if (!error) {
        // Capture exit code after kill if possible.
        Wait();
      } else if (error) {
        RAY_LOG(DEBUG) << "Failed to kill process " << pid << " with error " << error
                       << ": " << error.message();
      }
    } else {
      // (Dummy process case)
      // Theoretically we could keep around an exit code here for Wait() to return,
      // but we might as well pretend this fake process had already finished running.
      // So don't bother doing anything.
    }
  } else {
    // (Null process case)
  }
}

#ifdef _WIN32
#ifndef STATUS_BUFFER_OVERFLOW
#define STATUS_BUFFER_OVERFLOW ((NTSTATUS)0x80000005L)
#endif
typedef LONG NTSTATUS;
typedef NTSTATUS WINAPI NtQueryInformationProcess_t(HANDLE ProcessHandle,
                                                    ULONG ProcessInformationClass,
                                                    PVOID ProcessInformation,
                                                    ULONG ProcessInformationLength,
                                                    ULONG *ReturnLength);

static std::atomic<NtQueryInformationProcess_t *> NtQueryInformationProcess_ = NULL;

pid_t GetParentPID() {
  NtQueryInformationProcess_t *NtQueryInformationProcess = NtQueryInformationProcess_;
  if (!NtQueryInformationProcess) {
    NtQueryInformationProcess = reinterpret_cast<NtQueryInformationProcess_t *>(
        GetProcAddress(GetModuleHandle(TEXT("ntdll.dll")),
                       _CRT_STRINGIZE(NtQueryInformationProcess)));
    NtQueryInformationProcess_ = NtQueryInformationProcess;
  }
  DWORD ppid = 0;
  PROCESS_BASIC_INFORMATION info;
  ULONG cb = sizeof(info);
  NTSTATUS status = NtQueryInformationProcess(GetCurrentProcess(), 0, &info, cb, &cb);
  if ((status >= 0 || status == STATUS_BUFFER_OVERFLOW) && cb >= sizeof(info)) {
    ppid = static_cast<DWORD>(reinterpret_cast<uintptr_t>(info.Reserved3));
  }
  pid_t result = 0;
  if (ppid > 0) {
    // For now, assume PPID = 1 (simulating the reassignment to "init" on Linux)
    result = 1;
    if (HANDLE parent = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, ppid)) {
      long long me_created, parent_created;
      FILETIME unused;
      if (GetProcessTimes(GetCurrentProcess(),
                          reinterpret_cast<FILETIME *>(&me_created),
                          &unused,
                          &unused,
                          &unused) &&
          GetProcessTimes(parent,
                          reinterpret_cast<FILETIME *>(&parent_created),
                          &unused,
                          &unused,
                          &unused)) {
        if (me_created >= parent_created) {
          // We verified the child is younger than the parent, so we know the parent
          // is still alive.
          // (Note that the parent can still die by the time this function returns,
          // but that race condition exists on POSIX too, which we're emulating here.)
          result = static_cast<pid_t>(ppid);
        }
      }
      CloseHandle(parent);
    }
  }
  return result;
}
#else
pid_t GetParentPID() { return getppid(); }
#endif  // #ifdef _WIN32

pid_t GetPID() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

bool IsParentProcessAlive() { return GetParentPID() != 1; }

bool IsProcessAlive(pid_t pid) {
#if defined _WIN32
  if (HANDLE handle =
          OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, static_cast<DWORD>(pid))) {
    DWORD exit_code;
    if (GetExitCodeProcess(handle, &exit_code) && exit_code == STILL_ACTIVE) {
      return true;
    }
    CloseHandle(handle);
  }
  return false;
#else
  // Note if the process is a zombie (dead but not yet reaped), it will
  // still be alive by this check.
  if (kill(pid, 0) == -1 && errno == ESRCH) {
    return false;
  }
  return true;
#endif
}

#if defined(__linux__)
static inline std::error_code KillProcLinux(pid_t pid) {
  std::error_code error;
  if (kill(pid, SIGKILL) != 0) {
    error = std::error_code(errno, std::system_category());
  }
  return error;
}
#endif

std::optional<std::error_code> KillProc(pid_t pid) {
#if defined(__linux__)
  return {KillProcLinux(pid)};
#else
  return std::nullopt;
#endif
}

#if defined(__linux__)
static inline std::vector<pid_t> GetAllProcsWithPpidLinux(pid_t parent_pid) {
  std::vector<pid_t> child_pids;

  // Iterate over all files in the /proc directory, looking for directories.
  // See `man proc` for information on the directory structure.
  // Directories with only digits in their name correspond to processes in the process
  // table. We read in the status of each such process and parse the parent PID. If the
  // process parent PID is equal to parent_pid, then we add it to the vector to be
  // returned. Ideally, we use a library for this, but at the time of writing one is not
  // available in Ray C++.

  std::filesystem::directory_iterator dir(kProcDirectory);
  for (const auto &file : dir) {
    if (!file.is_directory()) {
      continue;
    }

    // Determine if the directory name consists of only digits (means it's a PID).
    const auto filename = file.path().filename().string();
    bool file_name_is_only_digit =
        std::all_of(filename.begin(), filename.end(), ::isdigit);
    if (!file_name_is_only_digit) {
      continue;
    }

    // If so, open the status file for reading.
    pid_t pid = std::stoi(filename);
    std::ifstream status_file(file.path() / "status");
    if (!status_file.is_open()) {
      continue;
    }

    // Scan for the line that starts with the ppid key.
    std::string line;
    const std::string key = "PPid:";
    while (std::getline(status_file, line)) {
      const auto substr = line.substr(0, key.size());
      if (substr != key) {
        continue;
      }

      // We found it, read and parse the PPID.
      pid_t ppid = std::stoi(line.substr(substr.size()));
      if (ppid == parent_pid) {
        child_pids.push_back(pid);
      }
      break;
    }
  }

  return child_pids;
}
#endif

std::optional<std::vector<pid_t>> GetAllProcsWithPpid(pid_t parent_pid) {
#if defined(__linux__)
  return {GetAllProcsWithPpidLinux(parent_pid)};
#else
  return std::nullopt;
#endif
}

void QuickExit() {
  ray::RayLog::ShutDownRayLog();
  _Exit(1);
}

}  // namespace ray

namespace std {

bool equal_to<ray::Process>::operator()(const ray::Process &x,
                                        const ray::Process &y) const {
  using namespace ray;  // NOLINT
  return !x.IsNull()
             ? !y.IsNull()
                   ? x.IsValid()
                         ? y.IsValid() ? equal_to<pid_t>()(x.GetId(), y.GetId()) : false
                     : y.IsValid() ? false
                                   : equal_to<void const *>()(x.Get(), y.Get())
                   : false
             : y.IsNull();
}

size_t hash<ray::Process>::operator()(const ray::Process &value) const {
  using namespace ray;  // NOLINT
  return !value.IsNull() ? value.IsValid() ? hash<pid_t>()(value.GetId())
                                           : hash<void const *>()(value.Get())
                         : size_t();
}

}  // namespace std
