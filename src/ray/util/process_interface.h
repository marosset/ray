// Copyright 2026 The Ray Authors.
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

#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/util/compat.h"

namespace ray {

using ProcessEnvironment = absl::flat_hash_map<std::string, std::string>;

/**
 * @brief Structured result from waiting for a process to exit.
 *
 * @details This type separates the fact that a process has exited from the
 *          platform-specific details describing how it exited. Some process
 *          tracking paths can observe death but cannot recover a raw exit code
 *          or signal. In those cases, process_exited is true but
 *          ExitStatusKnown() is false.
 */
struct ProcessExitStatus {
  /**
   * @brief Creates a status for paths where no exit code or signal is known.
   * @param pid The process ID associated with the wait result.
   * @param process_exited Whether the wait path observed process termination.
   * @param legacy_wait_status The integer status returned by legacy Wait().
   */
  static ProcessExitStatus Unknown(pid_t pid,
                                   bool process_exited = false,
                                   int legacy_wait_status = -1) {
    ProcessExitStatus status;
    status.pid = pid;
    status.process_exited = process_exited;
    status.legacy_wait_status = legacy_wait_status;
    return status;
  }

  /**
   * @brief Creates a status for a process that exited normally with a code.
   * @param pid The process ID associated with the wait result.
   * @param exit_code The platform exit code.
   * @param legacy_wait_status The integer status returned by legacy Wait().
   */
  static ProcessExitStatus Exited(pid_t pid,
                                  int64_t exit_code,
                                  int legacy_wait_status) {
    ProcessExitStatus status;
    status.pid = pid;
    status.process_exited = true;
    status.exit_code_known = true;
    status.exit_code = exit_code;
    status.legacy_wait_status = legacy_wait_status;
    return status;
  }

  /**
   * @brief Creates a status for a process terminated by a POSIX signal.
   * @param pid The process ID associated with the wait result.
   * @param termination_signal The terminating signal number.
   * @param legacy_wait_status The raw wait status returned by legacy Wait().
   */
  static ProcessExitStatus Signaled(pid_t pid,
                                    int termination_signal,
                                    int legacy_wait_status) {
    ProcessExitStatus status;
    status.pid = pid;
    status.process_exited = true;
    status.termination_signal_known = true;
    status.termination_signal = termination_signal;
    status.legacy_wait_status = legacy_wait_status;
    return status;
  }

  /**
   * @brief Whether the result includes a known exit code or terminating signal.
   */
  bool ExitStatusKnown() const {
    return exit_code_known || termination_signal_known;
  }

  /// Process ID associated with the wait result, or -1 for a null process.
  pid_t pid = -1;

  /// True when the wait path observed that the process has terminated.
  bool process_exited = false;

  /// True when exit_code contains a platform exit code.
  bool exit_code_known = false;

  /// Platform exit code for normal process termination.
  int64_t exit_code = 0;

  /// True when termination_signal contains a POSIX terminating signal.
  bool termination_signal_known = false;

  /// POSIX signal number that terminated the process.
  int termination_signal = 0;

  /// True when raw_wait_status contains the raw POSIX waitpid status.
  bool raw_wait_status_known = false;

  /// Raw POSIX waitpid status, preserved for callers that need platform detail.
  int raw_wait_status = 0;

  /// Integer returned by the legacy Wait() API for compatibility.
  int legacy_wait_status = -1;
};

/**
 * @class ProcessInterface
 * @details The Implementations of this interface are used to track the lifetime
 *          of the underlying OS process, and provides wrappers to the system
 *          calls to interact with the process.
 */
class ProcessInterface {
 public:
  virtual ~ProcessInterface() = default;

  /**
   * @brief Get the process ID.
   * @return The process ID, or -1 for a null process.
   */
  virtual pid_t GetId() const = 0;

  /**
   * @brief Check if this is a null process object.
   * @return True if the process is null, false otherwise.
   */
  virtual bool IsNull() const = 0;

  /**
   * @brief Check if this process has a valid (non-negative) PID.
   * @return True if the process is valid, false otherwise.
   */
  virtual bool IsValid() const = 0;

  /**
   * @brief Forcefully kills the process.
   * @details It is unsafe to kill unowned processes (processes created outside of raylet)
   *          as their death may not be tracked by the parent process and can result
   *          in double kill attempts.
   */
  virtual void Kill() = 0;

  /**
   * @brief Check whether the process is alive.
   * @return True if the process is alive, false otherwise.
   */
  virtual bool IsAlive() const = 0;

  /**
   * @brief Waits for process to terminate.
   * @details Wait can only be called on processes spawned by this Process instance
   *          either as a child process or a decoupled grandchild process.
   *          Calling wait on a process with no relationship to this Process
   *          will result in error being returned.
   *
   * @return The process's exit code. Returns -1 for a null process.
   */
  virtual int Wait() const = 0;

  /**
   * @brief Waits for process termination and returns structured exit information.
   * @details The status includes an exit code or termination signal when the platform
    *          and process relationship can provide one. Some existing POSIX spawn paths
    *          can only observe that the process exited through a liveness pipe. The
    *          default implementation preserves compatibility by calling Wait() and
    *          returning an unknown structured status with the legacy wait value.
   */
  virtual ProcessExitStatus WaitForExit() const {
    return ProcessExitStatus::Unknown(GetId(), /*process_exited=*/true, Wait());
  }
};

}  // namespace ray
