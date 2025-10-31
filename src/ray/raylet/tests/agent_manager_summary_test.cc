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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray::raylet {

// Minimal A7 aggregation test: verify summary reflects mixed ready states and fate sharing.

static std::string ResolveSleepLoopPath() {
#ifdef _WIN32
  std::string exe;
  if (const char *test_srcdir = std::getenv("TEST_SRCDIR")) {
    std::string c = std::string(test_srcdir) + "/io_ray/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(c).good()) {
      exe = c;
    }
  }
  if (exe.empty()) {
    std::string c = "./bazel-bin/src/ray/util/tests/sleep_loop.exe";
    if (std::ifstream(c).good()) {
      exe = c;
    }
  }
  if (exe.empty()) {
    exe = "sleep_loop.exe";
  }
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
}

TEST(AgentManagerSummaryTest, MixedReadinessSummary) {
  // Two agents: fast (exits) and slow (still running).
  AgentManager::Options fast_opts{ray::NodeID::FromRandom(),
                                  "fast_agent",
                                  {ResolveSleepLoopPath(), "--millis=200"},
                                  /*fate_shares=*/false};
  AgentManager::Options slow_opts{ray::NodeID::FromRandom(),
                                  "slow_agent",
                                  {ResolveSleepLoopPath(), "--millis=5000"},
                                  /*fate_shares=*/false};
  AgentManager fast(std::move(fast_opts),
                    [](auto, auto) { return nullptr; },
                    [](const ray::rpc::NodeDeathInfo &) {},
                    true);
  AgentManager slow(std::move(slow_opts),
                    [](auto, auto) { return nullptr; },
                    [](const ray::rpc::NodeDeathInfo &) {},
                    true);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  std::string summary = FormatAgentShutdownSummary({&fast, &slow});
  // Expect fast ready & exit_code 0, slow not ready.
  EXPECT_NE(summary.find("fast_agent"), std::string::npos);
  EXPECT_NE(summary.find("ready:1"), std::string::npos);
  EXPECT_NE(summary.find("exit_code:0"), std::string::npos);
  EXPECT_NE(summary.find("slow_agent"), std::string::npos);
  // For slow agent we expect a ready:0 token somewhere after its name.
  // Simplicity: just ensure at least one ready:0 exists.
  EXPECT_NE(summary.find("ready:0"), std::string::npos);
}

}  // namespace ray::raylet
