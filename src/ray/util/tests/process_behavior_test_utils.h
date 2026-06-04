#pragma once

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include "ray/util/logging.h"

namespace ray::test {

inline std::string NormalizeRunfileKey(std::string path) {
  for (char &character : path) {
    if (character == '\\') {
      character = '/';
    }
  }
  return path;
}

inline bool EndsWith(const std::string &value, const std::string &suffix) {
  return value.size() >= suffix.size() &&
         value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline std::string ResolveFromManifest(const std::string &logical_path) {
  const char *manifest_path = std::getenv("RUNFILES_MANIFEST_FILE");
  if (manifest_path == nullptr) {
    return "";
  }
  std::ifstream manifest(manifest_path);
  std::string line;
  const std::string normalized_logical_path = NormalizeRunfileKey(logical_path);
  while (std::getline(manifest, line)) {
    const auto separator = line.find(' ');
    if (separator == std::string::npos) {
      continue;
    }
    const std::string key = NormalizeRunfileKey(line.substr(0, separator));
    if (key == normalized_logical_path || EndsWith(key, "/" + normalized_logical_path)) {
      return line.substr(separator + 1);
    }
  }
  return "";
}

inline std::string ResolveRunfilePath(const std::string &logical_path) {
  if (std::filesystem::exists(logical_path)) {
    return logical_path;
  }
  const auto from_cwd = std::filesystem::current_path() / logical_path;
  if (std::filesystem::exists(from_cwd)) {
    return from_cwd.string();
  }
  const char *runfiles_dir = std::getenv("RUNFILES_DIR");
  if (runfiles_dir != nullptr) {
    const auto from_runfiles_dir = std::filesystem::path(runfiles_dir) / logical_path;
    if (std::filesystem::exists(from_runfiles_dir)) {
      return from_runfiles_dir.string();
    }
  }
  std::string from_manifest = ResolveFromManifest(logical_path);
  if (!from_manifest.empty()) {
    return from_manifest;
  }
  return logical_path;
}

inline std::string ProcessBehaviorHelperPath() {
  const char *helper_path = std::getenv("TEST_PROCESS_BEHAVIOR_HELPER_EXEC_PATH");
  RAY_CHECK(helper_path != nullptr)
      << "TEST_PROCESS_BEHAVIOR_HELPER_EXEC_PATH must be set by Bazel";
  return ResolveRunfilePath(helper_path);
}

inline bool WaitForFile(const std::filesystem::path &path,
                        std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::filesystem::exists(path)) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return std::filesystem::exists(path);
}

inline std::string ReadFile(const std::filesystem::path &path) {
  std::ifstream file(path);
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

}  // namespace ray::test