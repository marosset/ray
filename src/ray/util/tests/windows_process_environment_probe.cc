#include "process_behavior_test_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "ray/util/temporary_directory.h"

#ifdef _WIN32
#include <windows.h>
#endif

namespace ray {
namespace {

#ifdef _WIN32

std::string BoolString(bool value) { return value ? "true" : "false"; }

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

std::string BuildCommandLine(const std::vector<std::string> &args) {
  std::string command_line;
  for (const auto &arg : args) {
    if (!command_line.empty()) {
      command_line += " ";
    }
    command_line += QuoteArg(arg);
  }
  return command_line;
}

class ScopedProcess {
 public:
  ScopedProcess() = default;

  explicit ScopedProcess(PROCESS_INFORMATION process_info)
      : process_info_(process_info), valid_(true) {}

  ScopedProcess(const ScopedProcess &) = delete;
  ScopedProcess &operator=(const ScopedProcess &) = delete;

  ScopedProcess(ScopedProcess &&other) noexcept { MoveFrom(std::move(other)); }

  ScopedProcess &operator=(ScopedProcess &&other) noexcept {
    if (this != &other) {
      TerminateAndClose();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  ~ScopedProcess() { TerminateAndClose(); }

  bool IsValid() const { return valid_; }

  HANDLE ProcessHandle() const { return process_info_.hProcess; }

  HANDLE ThreadHandle() const { return process_info_.hThread; }

  DWORD ProcessId() const { return process_info_.dwProcessId; }

  bool WaitForExit(std::chrono::milliseconds timeout, DWORD *exit_code = nullptr) {
    if (!valid_) {
      return false;
    }
    const DWORD wait_result = WaitForSingleObject(
        process_info_.hProcess, static_cast<DWORD>(timeout.count()));
    if (wait_result != WAIT_OBJECT_0) {
      return false;
    }
    if (exit_code != nullptr) {
      GetExitCodeProcess(process_info_.hProcess, exit_code);
    }
    return true;
  }

  void Resume() {
    if (valid_ && process_info_.hThread != nullptr) {
      ResumeThread(process_info_.hThread);
    }
  }

  void TerminateAndClose() {
    if (!valid_) {
      return;
    }
    DWORD exit_code = 0;
    if (process_info_.hProcess != nullptr &&
        GetExitCodeProcess(process_info_.hProcess, &exit_code) &&
        exit_code == STILL_ACTIVE) {
      TerminateProcess(process_info_.hProcess, 1);
      WaitForSingleObject(process_info_.hProcess, 5000);
    }
    if (process_info_.hThread != nullptr) {
      CloseHandle(process_info_.hThread);
    }
    if (process_info_.hProcess != nullptr) {
      CloseHandle(process_info_.hProcess);
    }
    valid_ = false;
    process_info_ = {};
  }

 private:
  void MoveFrom(ScopedProcess &&other) {
    process_info_ = other.process_info_;
    valid_ = other.valid_;
    other.process_info_ = {};
    other.valid_ = false;
  }

  PROCESS_INFORMATION process_info_{};
  bool valid_ = false;
};

struct CreateProcessResult {
  ScopedProcess process;
  DWORD error = 0;
};

CreateProcessResult StartHelperProcess(const std::vector<std::string> &args,
                                       DWORD creation_flags) {
  std::vector<std::string> command = {test::ProcessBehaviorHelperPath()};
  command.insert(command.end(), args.begin(), args.end());
  std::string command_line = BuildCommandLine(command);
  std::vector<char> mutable_command_line(command_line.begin(), command_line.end());
  mutable_command_line.push_back('\0');

  STARTUPINFOA startup_info{};
  startup_info.cb = sizeof(startup_info);
  PROCESS_INFORMATION process_info{};
  SetLastError(0);
  const BOOL created = CreateProcessA(nullptr,
                                      mutable_command_line.data(),
                                      nullptr,
                                      nullptr,
                                      FALSE,
                                      creation_flags,
                                      nullptr,
                                      nullptr,
                                      &startup_info,
                                      &process_info);
  if (!created) {
    return CreateProcessResult{ScopedProcess(), GetLastError()};
  }
  return CreateProcessResult{ScopedProcess(process_info), 0};
}

#endif

TEST(WindowsProcessEnvironmentProbeTest, ReportsBazelJobObjectCapabilities) {
#if !defined(_WIN32)
  GTEST_SKIP() << "Windows process-environment probe only runs on Windows.";
#else
  BOOL in_job = FALSE;
  SetLastError(0);
  const BOOL queried_job = IsProcessInJob(GetCurrentProcess(), nullptr, &in_job);
  const DWORD queried_job_error = queried_job ? 0 : GetLastError();
  RecordProperty("is_process_in_job_query_succeeded", BoolString(queried_job));
  RecordProperty("is_process_in_job", BoolString(in_job));
  RecordProperty("is_process_in_job_error", static_cast<int>(queried_job_error));

  auto breakaway = StartHelperProcess({"--exit-code=0"}, CREATE_BREAKAWAY_FROM_JOB);
  RecordProperty("breakaway_create_process_succeeded",
                 BoolString(breakaway.process.IsValid()));
  RecordProperty("breakaway_create_process_error", static_cast<int>(breakaway.error));
  if (breakaway.process.IsValid()) {
    DWORD exit_code = 0;
    EXPECT_TRUE(breakaway.process.WaitForExit(std::chrono::seconds(5), &exit_code));
    RecordProperty("breakaway_exit_code", static_cast<int>(exit_code));
  }

  auto process_group = StartHelperProcess({"--exit-code=0"}, CREATE_NEW_PROCESS_GROUP);
  RecordProperty("create_new_process_group_succeeded",
                 BoolString(process_group.process.IsValid()));
  RecordProperty("create_new_process_group_error",
                 static_cast<int>(process_group.error));
  if (process_group.process.IsValid()) {
    DWORD exit_code = 0;
    EXPECT_TRUE(process_group.process.WaitForExit(std::chrono::seconds(5), &exit_code));
    RecordProperty("create_new_process_group_exit_code", static_cast<int>(exit_code));
  }

  HANDLE job = CreateJobObjectA(nullptr, nullptr);
  const DWORD create_job_error = job == nullptr ? GetLastError() : 0;
  RecordProperty("create_job_object_succeeded", BoolString(job != nullptr));
  RecordProperty("create_job_object_error", static_cast<int>(create_job_error));

  auto suspended = StartHelperProcess({"--sleep-ms=5000"}, CREATE_SUSPENDED);
  RecordProperty("assign_job_child_create_succeeded",
                 BoolString(suspended.process.IsValid()));
  RecordProperty("assign_job_child_create_error", static_cast<int>(suspended.error));
  if (job != nullptr && suspended.process.IsValid()) {
    SetLastError(0);
    const BOOL assigned = AssignProcessToJobObject(job, suspended.process.ProcessHandle());
    const DWORD assign_error = assigned ? 0 : GetLastError();
    RecordProperty("assign_process_to_job_object_succeeded", BoolString(assigned));
    RecordProperty("assign_process_to_job_object_error", static_cast<int>(assign_error));
    suspended.process.Resume();
  }
  if (job != nullptr) {
    CloseHandle(job);
  }
#endif
}

TEST(WindowsProcessEnvironmentProbeTest, ReportsCtrlBreakDeliveryToChildGroup) {
#if !defined(_WIN32)
  GTEST_SKIP() << "Windows process-environment probe only runs on Windows.";
#else
  ScopedTemporaryDirectory temp_dir;
  const auto ready_file = temp_dir.GetDirectory() / "ready.txt";
  const auto marker_file = temp_dir.GetDirectory() / "graceful.txt";

  auto child = StartHelperProcess({"--ready-file=" + ready_file.string(),
                                   "--marker-file=" + marker_file.string(),
                                   "--handle-graceful",
                                   "--graceful-exit-code=31",
                                   "--sleep-ms=5000"},
                                  CREATE_NEW_PROCESS_GROUP);
  RecordProperty("ctrl_break_child_create_succeeded",
                 BoolString(child.process.IsValid()));
  RecordProperty("ctrl_break_child_create_error", static_cast<int>(child.error));
  if (!child.process.IsValid()) {
    return;
  }

  ASSERT_TRUE(test::WaitForFile(ready_file, std::chrono::seconds(5)));
  SetLastError(0);
  const BOOL generated = GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT,
                                                  child.process.ProcessId());
  const DWORD generate_error = generated ? 0 : GetLastError();
  RecordProperty("generate_ctrl_break_succeeded", BoolString(generated));
  RecordProperty("generate_ctrl_break_error", static_cast<int>(generate_error));

  const bool marker_observed =
      generated && test::WaitForFile(marker_file, std::chrono::seconds(2));
  RecordProperty("ctrl_break_marker_observed", BoolString(marker_observed));
  if (marker_observed) {
    EXPECT_EQ(test::ReadFile(marker_file), "graceful\n");
    DWORD exit_code = 0;
    EXPECT_TRUE(child.process.WaitForExit(std::chrono::seconds(5), &exit_code));
    RecordProperty("ctrl_break_child_exit_code", static_cast<int>(exit_code));
  }
#endif
}

}  // namespace
}  // namespace ray