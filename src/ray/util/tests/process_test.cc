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
#include <thread>
#include <vector>

#include "process_behavior_test_utils.h"
#include "ray/util/fake_process.h"
#include "ray/util/logging.h"
#include "ray/util/process_utils.h"
#include "ray/util/temporary_directory.h"

#if !defined(_WIN32)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ray {

namespace {

std::unique_ptr<ProcessInterface> SpawnProcessBehaviorHelper(
    const std::vector<std::string> &args) {
  std::vector<std::string> command = {test::ProcessBehaviorHelperPath()};
  command.insert(command.end(), args.begin(), args.end());
  auto [process, error] = Process::Spawn(command, /*decouple=*/false);
  RAY_CHECK(!error) << error.message();
  RAY_CHECK(process->IsValid());
  return std::move(process);
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

TEST(UtilTest, CompareProcessObjects) {
  // Test the std::equal_to<Process> specialization with actual Process objects
  Process null1, null2;
  Process valid1(GetPID()), valid2(GetPID());  // Both reference the current process
  Process other_valid(1);                      // A different valid process (init/systemd)

  // Null process checks
  ASSERT_TRUE(null1.IsNull());
  ASSERT_TRUE(!null1.IsValid());
  ASSERT_TRUE(std::equal_to<Process>()(null1, null1));

  // Valid process checks
  ASSERT_TRUE(!valid1.IsNull());
  ASSERT_TRUE(valid1.IsValid());

  ASSERT_TRUE(std::equal_to<Process>()(null1, null2));
  ASSERT_TRUE(!std::equal_to<Process>()(null1, valid1));

  ASSERT_TRUE(!std::equal_to<Process>()(valid1, null1));
  ASSERT_TRUE(std::equal_to<Process>()(valid1, valid2));
  ASSERT_TRUE(!std::equal_to<Process>()(valid1, other_valid));

  ASSERT_TRUE(std::equal_to<Process>()(valid1, valid1));
}

TEST(UtilTest, FakeProcessTracksActionOrdering) {
  FakeProcess process(/*pid=*/1234);
  process.SetExitCode(17);

  EXPECT_EQ(process.KillCallCount(), 0);
  EXPECT_EQ(process.WaitCallCount(), 0);
  EXPECT_EQ(process.WaitForExitCallCount(), 0);
  EXPECT_EQ(process.IsAliveCallCount(), 0);
  EXPECT_EQ(process.GracefulTerminationRequestCount(), 0);
  EXPECT_EQ(process.GracefulTerminationUnsupportedCount(), 0);

  EXPECT_TRUE(process.IsAlive());
  EXPECT_EQ(process.IsAliveCallCount(), 1);
  process.RecordGracefulTerminationRequest("posix-sigterm");
  EXPECT_EQ(process.GracefulTerminationRequestCount(), 1);
  EXPECT_EQ(process.LastGracefulTerminationMechanism(), "posix-sigterm");
  const auto exit_status = process.WaitForExit();
  EXPECT_EQ(process.WaitForExitCallCount(), 1);
  EXPECT_TRUE(exit_status.process_exited);
  EXPECT_TRUE(exit_status.exit_code_known);
  EXPECT_EQ(exit_status.exit_code, 17);
  EXPECT_EQ(process.Wait(), 17);
  EXPECT_EQ(process.WaitCallCount(), 1);
  process.RecordGracefulTerminationUnsupported("windows-console-event");
  EXPECT_EQ(process.GracefulTerminationUnsupportedCount(), 1);
  EXPECT_EQ(process.LastGracefulTerminationMechanism(), "windows-console-event");

  process.Kill();
  EXPECT_TRUE(process.WasKilled());
  EXPECT_FALSE(process.IsAlive());
  EXPECT_EQ(process.KillCallCount(), 1);
  EXPECT_EQ(process.IsAliveCallCount(), 2);
  EXPECT_THAT(process.RecordedActions(),
              ::testing::ElementsAre("is_alive",
                                     "graceful",
                                     "wait_for_exit",
                                     "wait",
                                     "graceful_unsupported",
                                     "kill",
                                     "is_alive"));

  process.ResetKilled();
  process.ResetCallCounts();
  EXPECT_FALSE(process.WasKilled());
  EXPECT_EQ(process.KillCallCount(), 0);
  EXPECT_EQ(process.WaitCallCount(), 0);
  EXPECT_EQ(process.WaitForExitCallCount(), 0);
  EXPECT_EQ(process.IsAliveCallCount(), 0);
  EXPECT_EQ(process.GracefulTerminationRequestCount(), 0);
  EXPECT_EQ(process.GracefulTerminationUnsupportedCount(), 0);
  EXPECT_TRUE(process.LastGracefulTerminationMechanism().empty());
  EXPECT_TRUE(process.RecordedActions().empty());
}

TEST(UtilTest, ProcessBehaviorHelperExitsWithRequestedCode) {
  auto process = SpawnProcessBehaviorHelper({"--exit-code=23"});
  const auto status = process->WaitForExit();
  EXPECT_TRUE(status.process_exited);
#if defined(_WIN32)
  EXPECT_TRUE(status.exit_code_known);
  EXPECT_EQ(status.exit_code, 23);
  EXPECT_EQ(status.legacy_wait_status, 23);
#else
  // POSIX Process::Wait currently uses Ray's liveness pipe for spawned processes,
  // so it observes process death but does not preserve the raw exit code yet.
  EXPECT_FALSE(status.ExitStatusKnown());
  EXPECT_EQ(status.legacy_wait_status, 0);
#endif
}

TEST(UtilTest, ProcessWaitKeepsLegacyStatus) {
  auto process = SpawnProcessBehaviorHelper({"--exit-code=23"});
#if defined(_WIN32)
  EXPECT_EQ(process->Wait(), 23);
#else
  EXPECT_EQ(process->Wait(), 0);
#endif
}

TEST(UtilTest, ProcessWaitForExitReturnsStructuredStatusForPidWrapper) {
#if defined(_WIN32)
  GTEST_SKIP() << "Windows pid wrappers are not waitable without a process handle.";
#else
  pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    _exit(29);
  }

  Process process(pid);
  const auto status = process.WaitForExit();
  EXPECT_EQ(status.pid, pid);
  EXPECT_TRUE(status.process_exited);
  EXPECT_TRUE(status.exit_code_known);
  EXPECT_EQ(status.exit_code, 29);
  EXPECT_FALSE(status.termination_signal_known);
  EXPECT_TRUE(status.raw_wait_status_known);
  EXPECT_TRUE(WIFEXITED(status.raw_wait_status));
  EXPECT_EQ(WEXITSTATUS(status.raw_wait_status), 29);
  EXPECT_EQ(status.legacy_wait_status, status.raw_wait_status);
#endif
}

TEST(UtilTest, ProcessWaitForExitDecodesPosixSignalStatus) {
#if defined(_WIN32)
  GTEST_SKIP() << "POSIX signal status is not available on Windows.";
#else
  pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    raise(SIGTERM);
    _exit(1);
  }

  Process process(pid);
  const auto status = process.WaitForExit();
  EXPECT_EQ(status.pid, pid);
  EXPECT_TRUE(status.process_exited);
  EXPECT_FALSE(status.exit_code_known);
  EXPECT_TRUE(status.termination_signal_known);
  EXPECT_EQ(status.termination_signal, SIGTERM);
  EXPECT_TRUE(status.raw_wait_status_known);
  EXPECT_TRUE(WIFSIGNALED(status.raw_wait_status));
  EXPECT_EQ(WTERMSIG(status.raw_wait_status), SIGTERM);
  EXPECT_EQ(status.legacy_wait_status, status.raw_wait_status);
#endif
}

TEST(UtilTest, ProcessBehaviorHelperWritesReadyFile) {
  ScopedTemporaryDirectory temp_dir;
  const auto ready_file = temp_dir.GetDirectory() / "ready.txt";
  auto process = SpawnProcessBehaviorHelper(
      {"--ready-file=" + ready_file.string(), "--sleep-ms=5000"});
  ASSERT_TRUE(test::WaitForFile(ready_file, std::chrono::seconds(5)));
  EXPECT_EQ(test::ReadFile(ready_file), "ready\n");
  process->Kill();
  process->Wait();
}

TEST(UtilTest, ProcessBehaviorHelperWaitsForChildProcess) {
  ScopedTemporaryDirectory temp_dir;
  const auto child_marker_file = temp_dir.GetDirectory() / "child.txt";
  auto process = SpawnProcessBehaviorHelper({"--spawn-child=wait",
                                             "--child-marker-file=" +
                                                 child_marker_file.string()});
  const int status = process->Wait();
#if defined(_WIN32)
  EXPECT_EQ(status, 0);
#else
  EXPECT_EQ(status, 0);
#endif
  EXPECT_EQ(test::ReadFile(child_marker_file), "child-started\nchild-exiting\n");
}

TEST(UtilTest, ProcessBehaviorHelperCanLeaveBoundedChildRunning) {
  ScopedTemporaryDirectory temp_dir;
  const auto child_marker_file = temp_dir.GetDirectory() / "child.txt";
  auto process = SpawnProcessBehaviorHelper({"--spawn-child=leak",
                                             "--child-marker-file=" +
                                                 child_marker_file.string()});
  process->Wait();
  ASSERT_TRUE(test::WaitForFile(child_marker_file, std::chrono::seconds(5)));
  EXPECT_THAT(test::ReadFile(child_marker_file), ::testing::HasSubstr("child-started\n"));
}

TEST(UtilTest, ProcessBehaviorHelperHandlesPosixGracefulTermination) {
#if defined(_WIN32)
  GTEST_SKIP() << "Windows graceful console-event delivery is covered by a later 1B "
                  "Bazel compatibility probe.";
#else
  ScopedTemporaryDirectory temp_dir;
  const auto ready_file = temp_dir.GetDirectory() / "ready.txt";
  const auto marker_file = temp_dir.GetDirectory() / "graceful.txt";
  auto process = SpawnProcessBehaviorHelper({"--ready-file=" + ready_file.string(),
                                             "--marker-file=" + marker_file.string(),
                                             "--handle-graceful",
                                             "--graceful-exit-code=42",
                                             "--sleep-ms=5000"});

  ASSERT_TRUE(test::WaitForFile(ready_file, std::chrono::seconds(5)));
  ASSERT_EQ(kill(process->GetId(), SIGTERM), 0);
  ASSERT_TRUE(test::WaitForFile(marker_file, std::chrono::seconds(5)));
  EXPECT_EQ(test::ReadFile(marker_file), "graceful\n");
  EXPECT_EQ(process->Wait(), 0);
#endif
}

TEST(UtilTest, ProcessBehaviorHelperCanIgnorePosixGracefulTermination) {
#if defined(_WIN32)
  GTEST_SKIP() << "Windows graceful console-event delivery is covered by a later 1B "
                  "Bazel compatibility probe.";
#else
  ScopedTemporaryDirectory temp_dir;
  const auto ready_file = temp_dir.GetDirectory() / "ready.txt";
  auto process = SpawnProcessBehaviorHelper({"--ready-file=" + ready_file.string(),
                                             "--ignore-graceful",
                                             "--sleep-ms=5000"});

  ASSERT_TRUE(test::WaitForFile(ready_file, std::chrono::seconds(5)));
  ASSERT_EQ(kill(process->GetId(), SIGTERM), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(process->IsAlive());

  process->Kill();
  process->Wait();
#endif
}

}  // namespace ray
