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

#include "ray/raylet/agent_manager.h"

#include <chrono>
#include <fstream>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/util/logging.h"

namespace ray { namespace raylet {

// Resolve path to sleep_loop helper binary.
static std::string ResolveSleepLoop() {
#ifdef _WIN32
  std::string exe;
  if (const char *test_srcdir = std::getenv("TEST_SRCDIR")) {
    std::string candidate = std::string(test_srcdir) + "/io_ray/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(candidate).good()) { exe = candidate; }
  }
  if (exe.empty()) {
    std::string candidate = "./bazel-bin/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(candidate).good()) { exe = candidate; }
  }
  if (exe.empty()) { exe = "sleep_loop.exe"; }
  return exe;
#else
  return "sleep_loop";  // rely on runfiles
#endif
}

// Simple no-op delay executor used in tests WITHOUT fate sharing.
// (Fate-sharing tests below use RecordedDelayExecutor to observe scheduled QuickExit.)
static DelayExecutorFn NoopDelayExecutor() {
  return [](std::function<void()> fn, uint32_t ms) -> std::shared_ptr<boost::asio::deadline_timer> {
    (void)fn; (void)ms; return nullptr; };
}

TEST(AgentManagerTest, NaturalExitPublishesFuture) {
  std::vector<std::string> cmd{ResolveSleepLoop(), "--millis=500"}; // 0.5s
  AgentManager::Options opts{NodeID::FromRandom(), "test_agent", cmd, /*fate_shares=*/false};
  bool shutdown_called = false;
  AgentManager mgr(opts, NoopDelayExecutor(), [&](const rpc::NodeDeathInfo &){ shutdown_called = true; }, true);
  auto fut = TestingGetAgentExitFuture(mgr);
  ASSERT_TRUE(fut.valid());
  // Should become ready within a few seconds.
  auto status = fut.wait_for(std::chrono::seconds(5));
  ASSERT_EQ(status, std::future_status::ready);
  EXPECT_EQ(fut.get(), 0);
  EXPECT_FALSE(shutdown_called);
}

TEST(AgentManagerTest, KillTriggersFutureReadiness) {
  std::vector<std::string> cmd{ResolveSleepLoop(), "--millis=5000"}; // long running, we'll kill
  AgentManager::Options opts{NodeID::FromRandom(), "test_agent_kill", cmd, /*fate_shares=*/false};
  bool shutdown_called = false;
  auto mgr = std::make_unique<AgentManager>(opts, NoopDelayExecutor(), [&](const rpc::NodeDeathInfo &){ shutdown_called = true; }, true);
  auto fut = TestingGetAgentExitFuture(*mgr);
  ASSERT_TRUE(fut.valid());
  // Not expected to be ready immediately.
  auto early = fut.wait_for(std::chrono::milliseconds(100));
  EXPECT_NE(early, std::future_status::ready);
  // Destroy manager (kills process) and then future must become ready.
  mgr.reset();
  auto status = fut.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  // Exit code may be non-zero due to kill; just assert retrieval works.
  (void)fut.get();
  EXPECT_FALSE(shutdown_called);
}

// Helper to build a recording delay executor that captures scheduled QuickExit.
struct RecordedDelayExecutor {
  std::atomic<bool> scheduled{false};
  std::function<void()> fn;  // not invoked in tests
  DelayExecutorFn Make() {
    return [this](std::function<void()> f, uint32_t /*ms*/) -> std::shared_ptr<boost::asio::deadline_timer> {
      scheduled.store(true, std::memory_order_relaxed);
      fn = std::move(f);
      return nullptr;  // we don't need an actual timer
    };
  }
};

TEST(AgentManagerTest, FateShareNaturalExitTriggersShutdown) {
  std::vector<std::string> cmd{ResolveSleepLoop(), "--millis=200"};
  AgentManager::Options opts{NodeID::FromRandom(), "fate_share_agent", cmd, /*fate_shares=*/true};
  std::atomic<bool> shutdown_called{false};
  auto recorder = std::make_shared<RecordedDelayExecutor>();
  {
    AgentManager mgr(opts,
                     recorder->Make(),
                     [&](const rpc::NodeDeathInfo &){ shutdown_called.store(true, std::memory_order_relaxed); },
                     true);
    auto fut = TestingGetAgentExitFuture(mgr);
    ASSERT_TRUE(fut.valid());
    auto status = fut.wait_for(std::chrono::seconds(5));
    ASSERT_EQ(status, std::future_status::ready);
    // This should be natural exit (sleep finished) so exit code 0.
    EXPECT_EQ(fut.get(), 0);
    // Give the fate-share observer a brief window to run.
    for (int i = 0; i < 50 && !shutdown_called.load(std::memory_order_relaxed); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(shutdown_called.load(std::memory_order_relaxed));
    EXPECT_TRUE(recorder->scheduled.load(std::memory_order_relaxed));
    // We intentionally do NOT invoke recorder->fn() (would QuickExit()).
  }
  // After destruction no additional actions should have occurred (already asserted).
}

TEST(AgentManagerTest, FateShareDisabledOnDestructorKill) {
  std::vector<std::string> cmd{ResolveSleepLoop(), "--millis=5000"};
  AgentManager::Options opts{NodeID::FromRandom(), "fate_share_kill_agent", cmd, /*fate_shares=*/true};
  std::atomic<bool> shutdown_called{false};
  auto recorder = std::make_shared<RecordedDelayExecutor>();
  std::shared_future<int> fut;
  {
    auto mgr = std::make_unique<AgentManager>(opts,
                                              recorder->Make(),
                                              [&](const rpc::NodeDeathInfo &){ shutdown_called.store(true, std::memory_order_relaxed); },
                                              true);
    fut = TestingGetAgentExitFuture(*mgr);
    ASSERT_TRUE(fut.valid());
    // Destroy before natural exit; destructor disables fate sharing.
    mgr.reset();
  }
  auto status = fut.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready);
  (void)fut.get();
  EXPECT_FALSE(shutdown_called.load(std::memory_order_relaxed));
  EXPECT_FALSE(recorder->scheduled.load(std::memory_order_relaxed));
}

}} // namespace ray::raylet
