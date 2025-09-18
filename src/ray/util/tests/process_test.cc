// Copyright 2020 The Ray Authors.
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/process/child.hpp>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ray/util/logging.h"

namespace ray {

TEST(UtilTest, IsProcessAlive) {
  namespace bp = boost::process;
  bp::child c("bash");
  auto pid = c.id();
  c.join();
  for (int i = 0; i < 5; ++i) {
    if (IsProcessAlive(pid)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  RAY_CHECK(!IsProcessAlive(pid));
}

TEST(UtilTest, GetAllProcsWithPpid) {
#if defined(__linux__)
  // Verify correctness by spawning several child processes,
  // then asserting that each PID is present in the output.

  namespace bp = boost::process;

  std::vector<bp::child> actual_child_procs;

  for (int i = 0; i < 10; ++i) {
    actual_child_procs.push_back(bp::child("bash"));
  }

  std::optional<std::vector<pid_t>> maybe_child_procs = GetAllProcsWithPpid(GetPID());

  // Assert optional has value.
  ASSERT_EQ(static_cast<bool>(maybe_child_procs), true);

  // Assert each actual process ID is contained in the returned vector.
  auto child_procs = *maybe_child_procs;
  for (auto &child_proc : actual_child_procs) {
    pid_t pid = child_proc.id();
    EXPECT_THAT(child_procs, ::testing::Contains(pid));
  }

  // Clean up each child proc.
  for (auto &child_proc : actual_child_procs) {
    child_proc.join();
  }
#else
  auto result = GetAllProcsWithPpid(1);
  ASSERT_EQ(result, std::nullopt);
#endif
}

TEST(UtilTest, ProcessExitCode) {
  Process proc = Process::CreateNewDummy();
  ASSERT_EQ(proc.ExitCode(), kStillRunning);
}

TEST(UtilTest, ProcessWaitCapturesExitCode) {
  // Spawn a simple process that exits with code 0 (bash -c 'exit 0').
  std::vector<std::string> args = {"bash", "-c", "exit 0"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  ASSERT_EQ(proc.ExitCode(), kStillRunning);
  int code = proc.Wait();
  ASSERT_EQ(code, 0);
  ASSERT_EQ(proc.ExitCode(), 0);
  // Idempotent second wait.
  ASSERT_EQ(proc.Wait(), 0);
}

TEST(UtilTest, ProcessKillSetsExitCode) {
  // Launch a long-running sleep and kill it.
  std::vector<std::string> args = {"bash", "-c", "sleep 5"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process proc = std::move(pair.first);
  ASSERT_EQ(proc.ExitCode(), kStillRunning);
  proc.Kill();
  int code = proc.ExitCode();
  ASSERT_NE(code, kStillRunning);
  // On POSIX a signal maps to 128 + signal; on Windows we used 1 in TerminateProcess.
  // Accept any non-still-running value.
}

#ifdef _WIN32
TEST(UtilTest, WindowsKillExitCodeIsErrorProcessAborted) {
  std::vector<std::string> args = {"cmd.exe", "/C", "ping -n 6 127.0.0.1 >NUL"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process proc = std::move(pair.first);
  ASSERT_EQ(proc.ExitCode(), kStillRunning);
  proc.Kill();
  int code = proc.ExitCode();
  // We passed ERROR_PROCESS_ABORTED to TerminateProcess.
  ASSERT_EQ(code, static_cast<int>(ERROR_PROCESS_ABORTED));
}
#endif

TEST(UtilTest, ProcessWaitIdempotent) {
  std::vector<std::string> args = {"bash", "-c", "exit 7"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  int first = proc.Wait();
  ASSERT_EQ(first, 7);
  // Second wait should return cached value quickly.
  int second = proc.Wait();
  ASSERT_EQ(second, 7);
  ASSERT_EQ(proc.ExitCode(), 7);
}

TEST(UtilTest, ProcessKillThenWaitMultiple) {
  std::vector<std::string> args = {"bash", "-c", "sleep 10"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  proc.Kill();
  int code1 = proc.ExitCode();
  ASSERT_NE(code1, kStillRunning);
  // Calling Wait after Kill should just return cached code.
  int code2 = proc.Wait();
  ASSERT_EQ(code1, code2);
}

TEST(UtilTest, DummyProcessWait) {
  Process dummy = Process::CreateNewDummy();
  // Dummy process uses pid -1; it's a sentinel and not considered a valid spawned process.
  ASSERT_FALSE(dummy.IsValid());
  ASSERT_EQ(dummy.ExitCode(), kStillRunning);
  int code = dummy.Wait();
  ASSERT_EQ(code, 0);  // Dummy mapped to 0 per implementation.
  ASSERT_EQ(dummy.ExitCode(), 0);
}

TEST(UtilTest, NullProcessBehavior) {
  Process null_proc;  // default constructed is null
  ASSERT_TRUE(null_proc.IsNull());
  ASSERT_EQ(null_proc.ExitCode(), kStillRunning);
  int code = null_proc.Wait();
  ASSERT_EQ(code, -1);  // Null mapped to -1 per implementation.
  ASSERT_EQ(null_proc.ExitCode(), -1);
}

#if !defined(_WIN32)
TEST(UtilTest, SignalTerminationMapping) {
  // Cause the process to kill itself with SIGKILL via another process.
  // We'll spawn a process that sleeps; we then send SIGKILL.
  std::vector<std::string> args = {"bash", "-c", "sleep 10"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  pid_t pid = pair.first.GetId();
  // Send SIGKILL directly.
  ::kill(pid, SIGKILL);
  int code = pair.first.Wait();
  // Expect 128 + SIGKILL (9) = 137
  ASSERT_EQ(code, 128 + SIGKILL);
  ASSERT_EQ(pair.first.ExitCode(), 128 + SIGKILL);
}
#endif

TEST(UtilTest, SpawnWritesPidFile) {
  // Use a temporary file in current working directory (test sandbox). Name should be unique.
  std::string pid_file = "test_spawn_pid_file.pid";
  // Ensure no stale file.
  std::remove(pid_file.c_str());
  std::vector<std::string> args = {"bash", "-c", "exit 0"};
  auto pair = Process::Spawn(args, /*decouple*/ false, pid_file);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  // Read file
  std::ifstream f(pid_file);
  ASSERT_TRUE(f.is_open());
  pid_t file_pid = -1;
  f >> file_pid;
  f.close();
  ASSERT_EQ(file_pid, proc.GetId());
  // Clean up
  std::remove(pid_file.c_str());
  proc.Wait();
}

}  // namespace ray

int main(int argc, char **argv) {
  int result = 0;
  if (argc > 1 && strcmp(argv[1], "--println") == 0) {
    // If we're given this special command, emit each argument on a new line
    for (int i = 2; i < argc; ++i) {
      fprintf(stdout, "%s\n", argv[i]);
    }
  } else {
    ::testing::InitGoogleTest(&argc, argv);
    result = RUN_ALL_TESTS();
  }
  return result;
}
