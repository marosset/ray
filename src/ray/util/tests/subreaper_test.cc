#include <gtest/gtest.h>

#ifdef __linux__

#include <boost/asio/io_context.hpp>

#include "ray/util/subreaper.h"

namespace ray {
namespace {

TEST(ProcessExitCallbackRegistryTest, NotifiesInlineWithoutIoContext) {
  const pid_t pid = 123450;
  bool invoked = false;
  const auto ready = RegisterProcessExitCallback(
      pid, nullptr, [&](int exit_code) { invoked = true; EXPECT_EQ(exit_code, 13); });
  EXPECT_FALSE(ready.has_value());

  ProcessExitCallbackRegistry::instance().Notify(pid, 13);
  EXPECT_TRUE(invoked);

  RemoveProcessExitCallback(pid);
}

TEST(ProcessExitCallbackRegistryTest, RegisterAfterExitReturnsExitCode) {
  const pid_t pid = 123451;
  ProcessExitCallbackRegistry::instance().Notify(pid, 9);

  bool invoked = false;
  const auto ready = RegisterProcessExitCallback(
      pid, nullptr, [&](int) { invoked = true; });
  ASSERT_TRUE(ready.has_value());
  EXPECT_EQ(*ready, 9);
  EXPECT_FALSE(invoked);

  RemoveProcessExitCallback(pid);
}

TEST(ProcessExitCallbackRegistryTest, PostsCallbackToIoContext) {
  const pid_t pid = 123452;
  boost::asio::io_context io_context;

  bool invoked = false;
  const auto ready = RegisterProcessExitCallback(
      pid, &io_context, [&](int exit_code) { invoked = true; EXPECT_EQ(exit_code, 5); });
  EXPECT_FALSE(ready.has_value());

  ProcessExitCallbackRegistry::instance().Notify(pid, 5);
  EXPECT_FALSE(invoked);
  EXPECT_GT(io_context.poll(), 0);
  EXPECT_TRUE(invoked);

  RemoveProcessExitCallback(pid);
}

TEST(ProcessExitCallbackRegistryTest, RemoveSuppressesRegisteredCallback) {
  const pid_t pid = 123453;
  bool invoked = false;

  const auto ready = RegisterProcessExitCallback(
      pid, nullptr, [&](int) { invoked = true; });
  EXPECT_FALSE(ready.has_value());

  RemoveProcessExitCallback(pid);
  ProcessExitCallbackRegistry::instance().Notify(pid, 17);
  EXPECT_FALSE(invoked);

  const auto cached = RegisterProcessExitCallback(
      pid, nullptr, [&](int) { invoked = true; });
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(*cached, 17);
  EXPECT_FALSE(invoked);

  RemoveProcessExitCallback(pid);
}

}  // namespace
}  // namespace ray

#else  // __linux__

TEST(ProcessExitCallbackRegistryTest, SkipOnUnsupportedPlatform) {
  GTEST_SKIP() << "ProcessExitCallbackRegistry is only used on Linux.";
}

#endif  // __linux__
