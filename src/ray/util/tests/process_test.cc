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
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ray/util/logging.h"

namespace ray {

namespace {

std::vector<std::string> MakeExitCommandArgs(int exit_code, bool delayed) {
  std::vector<std::string> args;
#ifdef _WIN32
  std::string command;
  if (delayed) {
    command = "timeout /T 1 /NOBREAK >NUL & exit " + std::to_string(exit_code);
  } else {
    command = "exit " + std::to_string(exit_code);
  }
  args = {"cmd.exe", "/C", command};
#else
  std::string command;
  if (delayed) {
    command = "sleep 1; exit " + std::to_string(exit_code);
  } else {
    command = "exit " + std::to_string(exit_code);
  }
  args = {"bash", "-c", command};
#endif
  return args;
}

std::string ResolveSleepLoopExecutable() {
#ifdef _WIN32
  const char *binary_name = "sleep_loop.exe";
#else
  const char *binary_name = "sleep_loop";
#endif
  std::vector<std::string> candidates;
  if (const char *test_srcdir = std::getenv("TEST_SRCDIR")) {
    candidates.emplace_back(std::string(test_srcdir) + "/io_ray/src/ray/util/tests/" +
                            binary_name);
  }
  candidates.emplace_back("./bazel-bin/src/ray/util/tests/" + std::string(binary_name));
  candidates.emplace_back(binary_name);
  for (const auto &path : candidates) {
    std::ifstream f(path, std::ios::binary);
    if (f.good()) {
      return path;
    }
  }
  return candidates.back();
}

std::vector<std::string> MakeSleepLoopArgs(int millis) {
  std::vector<std::string> args;
  args.emplace_back(ResolveSleepLoopExecutable());
  args.emplace_back("--millis=" + std::to_string(millis));
  return args;
}

}  // namespace

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
  //dummy process exit code is immediately 0.
  ASSERT_EQ(proc.ExitCode(), 0);
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
  ASSERT_EQ(dummy.ExitCode(), 0);
  int code = dummy.Wait();
  ASSERT_EQ(code, 0);
}

TEST(UtilTest, NullProcessBehavior) {
  Process null_proc;
  ASSERT_TRUE(null_proc.IsNull());
  // null process has immediate terminal code -1.
  ASSERT_EQ(null_proc.ExitCode(), -1);
  int code = null_proc.Wait();
  ASSERT_EQ(code, -1);
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

TEST(UtilTest, WaitAsyncReturnsReadyFutureIfAlreadyExited) {
  std::vector<std::string> args = {"bash", "-c", "exit 42"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  int code = proc.Wait();
  ASSERT_EQ(code, 42);
  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  ASSERT_TRUE(fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
  ASSERT_EQ(fut.get(), 42);
}

TEST(UtilTest, WaitAsyncBasicCompletion) {
  std::vector<std::string> args = {"bash", "-c", "sleep 1; exit 3"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  // Wait with timeout loop to avoid hanging test in rare race.
  for (int i = 0; i < 50 &&
                  fut.wait_for(std::chrono::milliseconds(100)) !=
                      std::future_status::ready; ++i) {
  }
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
  ASSERT_EQ(fut.get(), 3);
  // ExitCode should now be updated.
  ASSERT_EQ(proc.ExitCode(), 3);
}

TEST(UtilTest, WaitAsyncMultipleConsumersShareSameFuture) {
  std::vector<std::string> args = {"bash", "-c", "sleep 1; exit 5"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  auto f1 = proc.WaitAsync();
  auto f2 = proc.WaitAsync();
  ASSERT_TRUE(f1.valid());
  ASSERT_TRUE(f2.valid());
  // They should become ready around the same time with same value.
  f1.wait();
  f2.wait();
  ASSERT_EQ(f1.get(), 5);
  ASSERT_EQ(proc.ExitCode(), 5);
  // Future remains valid for further get() via shared_future semantics.
  ASSERT_EQ(f2.get(), 5);
}

TEST(UtilTest, WaitAsyncKillPath) {
  std::vector<std::string> args = {"bash", "-c", "sleep 10"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  auto fut = proc.WaitAsync();
  proc.Kill();
  // Wait for future to observe killed exit code.
  for (int i = 0; i < 50 &&
                  fut.wait_for(std::chrono::milliseconds(100)) !=
                      std::future_status::ready; ++i) {
  }
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
  int code = fut.get();
#ifdef _WIN32
  ASSERT_EQ(code, static_cast<int>(ERROR_PROCESS_ABORTED));
#else
  // Could be 128 + SIGKILL if direct PID wait, or 0 if pipe mode; accept non-still-running.
  ASSERT_NE(code, kStillRunning);
#endif
}

TEST(UtilTest, WaitAsyncIdempotentFutureReference) {
  std::vector<std::string> args = {"bash", "-c", "exit 9"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  auto f1 = proc.WaitAsync();
  auto f2 = proc.WaitAsync();
  f1.wait();
  f2.wait();
  ASSERT_EQ(f1.get(), 9);
  ASSERT_EQ(f2.get(), 9);
  ASSERT_EQ(proc.ExitCode(), 9);
}

TEST(UtilTest, WaitAsyncSyncAfterAsync) {
  std::vector<std::string> args = {"bash", "-c", "sleep 1; exit 11"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  // Invoke synchronous wait while async waiter thread may also be waiting.
  int code = proc.Wait();
  ASSERT_EQ(code, 11);
  // Future should now be ready with same value.
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
  ASSERT_EQ(fut.get(), 11);
  ASSERT_EQ(proc.ExitCode(), 11);
}

TEST(UtilTest, WaitAsyncImmediateDummy) {
  Process dummy = Process::CreateNewDummy();
  auto fut = dummy.WaitAsync();
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
  ASSERT_EQ(fut.get(), 0);
  ASSERT_EQ(dummy.ExitCode(), 0);
}

TEST(UtilTest, WaitAsyncImmediateNull) {
  Process null_proc;  // default constructed
  ASSERT_TRUE(null_proc.IsNull());
  auto fut = null_proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
  ASSERT_EQ(fut.get(), -1);
  ASSERT_EQ(null_proc.ExitCode(), -1);
}

TEST(UtilTest, WaitAsyncProcessAlreadyExitedWithoutPriorWait) {
  // Spawn a helper that exits immediately, then wait long enough to ensure it has
  // terminated before registering the async wait. This exercises the path where
  // WaitAsync() must observe a process that is already gone without any prior Wait().
  auto args = MakeSleepLoopArgs(0);
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  int code = fut.get();
  ASSERT_EQ(code, 0);
  ASSERT_EQ(proc.ExitCode(), 0);
  ASSERT_EQ(proc.Wait(), 0);
}

TEST(UtilTest, WaitAsyncFutureSurvivesProcessScope) {
  const int expected = 0;
  std::shared_future<int> fut;
  {
    auto args = MakeSleepLoopArgs(500);
    auto pair = Process::Spawn(args, /*decouple*/ false);
    ASSERT_FALSE(pair.second) << pair.second.message();
    fut = pair.first.WaitAsync();
  }
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(fut.get(), expected);
}

TEST(UtilTest, WaitAsyncSupportsRvalueProcess) {
  const int expected = 0;
  auto spawn_future = [expected]() {
    auto args = MakeSleepLoopArgs(500);
    auto pair = Process::Spawn(args, /*decouple*/ false);
    EXPECT_FALSE(pair.second) << pair.second.message();
    Process proc = std::move(pair.first);
    return std::move(proc).WaitAsync();
  };

  auto fut = spawn_future();
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(fut.get(), expected);
}

TEST(UtilTest, WaitAsyncSupportsDecoupledSpawn) {
  // Verify decoupled processes still resolve via WaitAsync() and Wait(). On POSIX the
  // exit code is not propagated (pipe close = 0), while on Windows we observe the real
  // exit status; both should agree on zero given the helper executable.
  auto args = MakeSleepLoopArgs(200);
  auto pair = Process::Spawn(args, /*decouple*/ true);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;

  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  int async_code = fut.get();
  ASSERT_EQ(async_code, 0);

  int wait_code = proc.Wait();
  ASSERT_EQ(wait_code, async_code);
  ASSERT_EQ(proc.ExitCode(), async_code);
}

TEST(UtilTest, WaitAsyncThenKillNoDoubleWait) {
  // Regression test for Windows race: WaitAsync() spawns a detached thread that waits
  // on the process handle. If Kill() is then called (which also waits), both threads
  // would try to wait on the same handle. On Windows, this fails with ERROR_INVALID_HANDLE.
  // The fix: Wait() checks if WaitAsync() is active and delegates to the future instead.
  std::vector<std::string> args = {"bash", "-c", "sleep 10"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  
  // Call WaitAsync first, spawning the detached waiter thread
  auto fut = proc.WaitAsync();
  ASSERT_TRUE(fut.valid());
  
  // Give the waiter thread a brief moment to start waiting
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  
  // Now kill the process. This internally calls Wait(), which must NOT
  // try to wait on the OS handle again (would fail on Windows).
  proc.Kill();
  
  // Future should become ready with killed exit code
  fut.wait();
  int code = fut.get();
#ifdef _WIN32
  ASSERT_EQ(code, static_cast<int>(ERROR_PROCESS_ABORTED));
#else
  ASSERT_NE(code, kStillRunning);
#endif
  
  // ExitCode should be updated
  ASSERT_EQ(proc.ExitCode(), code);
  
  // Additional Wait() calls should return cached value without error
  ASSERT_EQ(proc.Wait(), code);
}

TEST(UtilTest, MultipleWaitCallsWhileWaitAsyncActive) {
  // Test that multiple threads can call Wait() concurrently while WaitAsync()
  // is active, and all should properly delegate to the async waiter's future.
  std::vector<std::string> args = {"bash", "-c", "sleep 2; exit 13"};
  auto pair = Process::Spawn(args, /*decouple*/ false);
  ASSERT_FALSE(pair.second) << pair.second.message();
  Process &proc = pair.first;
  
  auto fut = proc.WaitAsync();
  
  // Spawn multiple threads that all call Wait()
  std::vector<std::thread> waiters;
  std::vector<int> results(3, kStillRunning);
  for (int i = 0; i < 3; ++i) {
    waiters.emplace_back([&proc, &results, i]() {
      results[i] = proc.Wait();
    });
  }
  
  // Wait for all threads to complete
  for (auto &t : waiters) {
    t.join();
  }
  
  fut.wait();
  int expected = fut.get();
  ASSERT_EQ(expected, 13);
  
  // All Wait() calls should have returned the same exit code
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(results[i], expected);
  }
  ASSERT_EQ(proc.ExitCode(), expected);
}

TEST(UtilTest, WaitAsyncManyParallelAsync) {
  const int kNum = 20;
  struct ProcFuturePair {
    Process proc;
    std::shared_future<int> fut;
    int expected;
  };
  std::vector<ProcFuturePair> entries;
  entries.reserve(kNum);
  for (int i = 0; i < kNum; ++i) {
    int expected = 20 + i;  // distinct exit code
  std::vector<std::string> args;
#ifdef _WIN32
  // Use cmd.exe to exit with a distinct code; introduce slight staggering via a post-spawn sleep.
  args = {"cmd.exe", "/C", "exit", std::to_string(expected)};
#else
  std::string cmd = "sleep 1; exit " + std::to_string(expected);
  args = {"bash", "-c", cmd};
#endif
    auto pair = Process::Spawn(args, /*decouple*/ false);
    ASSERT_FALSE(pair.second) << pair.second.message();
    // Move the process into the vector first so its address remains valid for the
    // lifetime of the waiter thread spawned by WaitAsync(). Calling WaitAsync() on a
    // temporary that is then moved-from (and destroyed at end-of-loop) results in the
    // waiter thread dereferencing a dead this pointer.
    entries.emplace_back();
    auto &slot = entries.back();
    slot.expected = expected;
    slot.proc = std::move(pair.first);
  slot.fut = slot.proc.WaitAsync();
  // Introduce a tiny delay between spawns on Windows to avoid all processes exiting
  // before any waiter threads have a chance to start (gives a more mixed timing profile)
#ifdef _WIN32
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
#endif
  }
  // Poll until all futures ready or timeout.
  const int kMaxLoops = 60;  // up to ~6s @100ms
  for (int loop = 0; loop < kMaxLoops; ++loop) {
    bool all_ready = true;
    for (auto &e : entries) {
      if (e.fut.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_ready = false;
        break;
      }
    }
    if (all_ready) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto &e : entries) {
    ASSERT_EQ(e.fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_EQ(e.fut.get(), e.expected);
    ASSERT_EQ(e.proc.ExitCode(), e.expected);
  }
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
