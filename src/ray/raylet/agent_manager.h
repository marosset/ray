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

#pragma once

#include <atomic>
#include <boost/asio/deadline_timer.hpp>
#include <csignal>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace raylet {

using DelayExecutorFn = std::function<std::shared_ptr<boost::asio::deadline_timer>(
    std::function<void()>, uint32_t)>;

// Manages a separate "Agent" process. In constructor (or the `StartAgent` method) it
// starts a process with `agent_commands` plus some additional arguments.
//
// Raylet outlives agents. This means when Raylet exits, the agents must exit; on the
// other hand, if the agent exits, the raylet may exit only if fate_shares = true. This is
// implement by the dtor of AgentManager to kill the agent process.
//
// We typically start these agents:
// - The DashboardAgent: `ray/dashboard/agent.py`
// - The RuntimeEnvAgent: `ray/_private/runtime_env/agent/main.py`
class AgentManager {
 public:
  struct Options {
    const NodeID node_id;
    const std::string agent_name;
    // Commands to start the agent.
    std::vector<std::string> agent_commands;
    // If true: the started process fate-shares with the raylet. i.e., when the process
    // fails to start or exits, we SIGTERM the raylet.
    bool fate_shares;
  };

  explicit AgentManager(
      Options options,
      DelayExecutorFn delay_executor,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
      bool start_agent = true /* for test */)
      : options_(std::move(options)),
        delay_executor_(std::move(delay_executor)),
        shutdown_raylet_gracefully_(std::move(shutdown_raylet_gracefully)),
        fate_shares_(options_.fate_shares) {
    if (options_.agent_name.empty()) {
      RAY_LOG(FATAL) << "AgentManager agent_name must not be empty.";
    }
    if (options_.agent_commands.empty()) {
      RAY_LOG(FATAL) << "AgentManager agent_commands must not be empty.";
    }
    if (start_agent) {
      StartAgent();
    }
  }
  ~AgentManager();

 private:
  void StartAgent();

 private:
  const Options options_;
  Process process_;
  DelayExecutorFn delay_executor_;
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;
  // If true, when the agent dies, raylet kills itself.
  std::atomic<bool> fate_shares_;
  // Shared future publishing the agent process exit code (WaitAsync contract).
  std::shared_future<int> exit_future_;
  // Thread that observes the published exit code to perform fate-sharing actions.
  std::unique_ptr<std::thread> fate_share_thread_;

  friend std::shared_future<int> TestingGetAgentExitFuture(AgentManager &mgr);
  friend std::string FormatAgentShutdownSummary(const std::vector<AgentManager *> &agents);
};

// Test-only accessor mirroring pattern used in other components (e.g., WorkerPool) to
// avoid expanding the public API surface.
inline std::shared_future<int> TestingGetAgentExitFuture(AgentManager &mgr) {
  return mgr.exit_future_;
}

// Produce a lightweight summary string for a set of AgentManager instances. Format:
// [{name:dashboard_agent,ready:1,exit_code:0,fate_shares:1}, ...]
// Non-blocking: futures polled with wait_for(0).
std::string FormatAgentShutdownSummary(const std::vector<AgentManager *> &agents);

}  // namespace raylet
}  // namespace ray
