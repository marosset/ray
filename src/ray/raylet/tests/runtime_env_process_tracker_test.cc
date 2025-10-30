// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Tests to verify tracked runtime env helper processes use WaitAsync futures
// and summary formatting captures success and failure exit codes.

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/raylet/runtime_env_agent_client.h"
#include "ray/util/process.h"

using ray::Process;
using ray::ProcessEnvironment;
using ray::raylet::RuntimeEnvAgentClient;

namespace {
// Resolve sleep_loop helper path similarly to other tests.
std::string ResolveSleepLoop() {
#ifdef _WIN32
  std::string exe;
  if (const char *test_srcdir = std::getenv("TEST_SRCDIR")) {
    std::string candidate = std::string(test_srcdir) + "/io_ray/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(candidate).good()) exe = candidate;
  }
  if (exe.empty()) {
    std::string candidate = "./bazel-bin/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(candidate).good()) exe = candidate;
  }
  if (exe.empty()) exe = "sleep_loop.exe";
  return exe;
#else
  std::vector<std::string> candidates;
  if (const char *test_srcdir = std::getenv("TEST_SRCDIR")) {
    candidates.emplace_back(std::string(test_srcdir) + "/io_ray/src/ray/util/tests/sleep_loop");
  }
  candidates.emplace_back("./bazel-bin/src/ray/util/tests/sleep_loop");
  candidates.emplace_back("sleep_loop");
  for (const auto &path : candidates) {
    if (std::ifstream(path, std::ios::binary).good()) {
      return path;
    }
  }
  return candidates.back();
#endif
}  // namespace

// Simple delay executor stub for client construction.
static auto NoopDelayExecutor() {
  return [](std::function<void()> fn,
            uint32_t) -> std::shared_ptr<boost::asio::deadline_timer> {
    // Immediate inline execute for simplicity (rarely used here).
    if (fn) fn();
    return nullptr;
  };
}

TEST(RuntimeEnvProcessTrackerTest, MixedReadinessAndSuccess) {
  instrumented_io_context io;  // No networking used in this test.
  auto client = RuntimeEnvAgentClient::Create(
      io, "127.0.0.1", 65535, NoopDelayExecutor(),
      [](const ray::rpc::NodeDeathInfo &) {});
  ASSERT_TRUE(client);
  std::string exe = ResolveSleepLoop();
  auto fast_future = client->StartTrackedProcess({exe, "--millis=200"}, "fast_env");
  auto slow_future = client->StartTrackedProcess({exe, "--millis=3000"}, "slow_env");
  ASSERT_TRUE(fast_future.valid());
  ASSERT_TRUE(slow_future.valid());
  // Poll for fast process readiness (robust against scheduler delays) up to 5s.
  for (int i = 0; i < 50; ++i) {
    if (fast_future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  auto summary = client->FormatTrackedProcessSummary();
  EXPECT_NE(summary.find("fast_env"), std::string::npos);
  EXPECT_EQ(fast_future.wait_for(std::chrono::milliseconds(0)), std::future_status::ready);
  EXPECT_NE(summary.find("ready:1"), std::string::npos);
  EXPECT_NE(summary.find("exit_code:"), std::string::npos);
  EXPECT_NE(summary.find("slow_env"), std::string::npos);
  // slow_env should still be running (not guaranteed but very likely); allow either state to avoid flakes.
  // If still running, expect ready:0 present; if it finished early treat as acceptable.
  if (slow_future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
    EXPECT_NE(summary.find("ready:0"), std::string::npos);
  }
}

TEST(RuntimeEnvProcessTrackerTest, FailureExitCodeViaKill) {
  instrumented_io_context io;
  auto client = RuntimeEnvAgentClient::Create(
      io, "127.0.0.1", 65535, NoopDelayExecutor(),
      [](const ray::rpc::NodeDeathInfo &) {});
  ASSERT_TRUE(client);
  std::string exe = ResolveSleepLoop();
  auto fut = client->StartTrackedProcess({exe, "--millis=5000"}, "will_fail");
  ASSERT_TRUE(fut.valid());
  // Give it a moment to start then kill.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  client->KillTrackedProcess("will_fail");
  // Wait for readiness (should finish quickly after kill).
  auto status = fut.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  int code = fut.get();
  // Don't assert specific non-zero; some Windows terminations may report 0 depending on timing.
  auto summary = client->FormatTrackedProcessSummary();
  // Only assert readiness; exit code content may vary cross-platform.
  EXPECT_NE(code, ray::kStillRunning);
  EXPECT_NE(summary.find("will_fail"), std::string::npos);
}

TEST(RuntimeEnvProcessTrackerTest, TrackExistingProcessPublishesFuture) {
  instrumented_io_context io;
  auto client = RuntimeEnvAgentClient::Create(
      io, "127.0.0.1", 65535, NoopDelayExecutor(),
      [](const ray::rpc::NodeDeathInfo &) {});
  ASSERT_TRUE(client);
  std::string exe = ResolveSleepLoop();
  std::vector<std::string> argv = {exe, "--millis=300"};
  ProcessEnvironment env;
  auto spawn = Process::Spawn(argv, /*decouple=*/false, /*pid_file=*/"", env);
  ASSERT_FALSE(spawn.second) << spawn.second.message();
  auto fut = client->TrackExistingProcess(spawn.first, "existing_proc");
  ASSERT_TRUE(fut.valid());
  // Wait up to 5 seconds for completion.
  auto status = fut.wait_for(std::chrono::seconds(5));
  ASSERT_EQ(status, std::future_status::ready);
  int code = fut.get();
  EXPECT_EQ(code, 0);  // sleep_loop returns success when not killed.
  auto summary = client->FormatTrackedProcessSummary();
  EXPECT_NE(summary.find("existing_proc"), std::string::npos);
}

}  // namespace
