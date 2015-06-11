/*
 * Copyright 2015 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <chrono>
#include <functional>
#include <future>
#include <thread>

#include "ConditionLock.hpp"
#include "TbbConditionLock.hpp"
#include "catch.hpp"

using std::async;
using std::chrono::milliseconds;
using std::condition_variable;
using std::future;
using std::future_status::ready;
using std::future_status::timeout;
using std::function;
using std::lock_guard;
using std::mutex;
using std::this_thread::yield;
using std::thread;
using std::unique_lock;

/** Repeat the test many times. */
void repeat(const function<void()> &body) {
  for (int i = 0; i < 100; ++i)
    body();
}

template <typename T>
void require_alive (future<T> &f) {
  yield();
  REQUIRE(f.wait_for(milliseconds(0)) == timeout);
}

template <typename T>
void require_ready (future<T> &f) {
  REQUIRE(f.wait_for(milliseconds(10)) == ready);
}

void lock_behaviors(const function<Lock*(void)> &new_lock) {

  SECTION("A lock should block a later writer", "[lock]") {
    repeat ([]{
      ConditionLock lock;
      lock.write(1);
      auto f = async([&lock] {
        return lock.write(2);
      });
      require_alive(f);
      lock.release(2);
      require_ready(f);
      REQUIRE(f.get() == 2);
    });
  }

  SECTION("A lock should block a later writer and raise the time", "[lock]") {
    repeat ([] {
      ConditionLock lock;
      lock.write(1);
      auto f = async([&lock] {
        return lock.write(2);
      });
      require_alive(f);
      lock.release(3);
      require_ready(f);
      REQUIRE(f.get() == 3);
    });
  }

  SECTION("A lock should block an earlier writer and raise the time", "[lock]") {
    repeat ([] {
      ConditionLock lock;
      lock.write(2);
      auto f = async([&lock] {
        return lock.write(1);
      });
      require_alive(f);
      lock.release(3);
      require_ready(f);
      REQUIRE(f.get() == 3);
    });
  }

  SECTION("A lock should block a later reader", "[lock]") {
    repeat ([] {
      ConditionLock lock;
      lock.write(1);
      auto f = async([&lock] {
        lock.read(2);
        return 0;
      });
      require_alive(f);
      lock.release(2);
      require_ready(f);
      REQUIRE(f.get() == 0);
    });
  }

  SECTION("A lock should block a later reader and raise the time", "[lock]") {
    repeat ([] {
      ConditionLock lock;
      lock.write(1);
      auto f = async([&lock] {
        lock.read(3);
        return 0;
      });
      require_alive(f);
      lock.release(2);
      require_ready(f);
      REQUIRE(f.get() == 0);
      REQUIRE(lock.write(2) == 3);
    });
  }

  SECTION("A lock should permit an earlier reader", "[lock]") {
    repeat ([] {
      ConditionLock lock;
      lock.write(1);
      auto f = async([&lock] {
        lock.read(1);
        return 0;
      });
      require_ready(f);
      REQUIRE(f.get() == 0);
    });
  }
}

TEST_CASE ("The ConditionLock should work", "[locks]") {
  lock_behaviors([] {
    return new ConditionLock();
  });
}

TEST_CASE ("The TbbConditionLock should work", "[locks]") {
  lock_behaviors([] {
    return new TbbConditionLock();
  });
}
