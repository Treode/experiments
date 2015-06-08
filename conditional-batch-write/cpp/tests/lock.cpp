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
#include <thread>

#include "ConditionLock.hpp"
#include "catch.hpp"

using std::chrono::milliseconds;
using std::condition_variable;
using std::function;
using std::lock_guard;
using std::mutex;
using std::this_thread::yield;
using std::thread;
using std::unique_lock;

/** A thread that signals its exit. */
class spawn {

  public:

    explicit spawn(const function<uint32_t()> &body):
      child([this, &body] {
        auto result = body();
        lock_guard<mutex> acqn(lock);
        exited = true;
        actual = result;
        cond.notify_one();
      })
    {
      child.detach();
    }

    /** Must be invoked in the main thread. */
    void assert_alive() {
      yield();
      lock_guard<mutex> acqn(lock);
      REQUIRE(!exited);
    }

    /** Must be invoked in the main thread. */
    void assert_exited (uint32_t expected) {
      unique_lock<mutex> acqn(lock);
      if (!exited)
        cond.wait_for(acqn, milliseconds(10));
      REQUIRE(exited);
      REQUIRE(actual == expected);
    }

  private:

    mutex lock;
    condition_variable cond;
    bool exited = false;
    uint32_t actual = 0;

    // This must be declared last so that it is initialize (started) after the other members have
    // been initialized. Otherwise, we create opportunites for races.
    thread child;
};

/** Repeat the test many times. */
void repeat(const function<void()> &body) {
  for (int i = 0; i < 100; ++i)
    body();
}

TEST_CASE("A lock should block a later writer", "[lock]") {
  repeat ([]{
    ConditionLock lock;
    lock.write(1);
    spawn t([&lock] {
      return lock.write(2);
    });
    t.assert_alive();
    lock.release(2);
    t.assert_exited(2);
  });
}

TEST_CASE("A lock should block a later writer and raise the time", "[lock]") {
  repeat ([] {
    ConditionLock lock;
    lock.write(1);
    spawn t([&lock] {
      return lock.write(2);
    });
    t.assert_alive();
    lock.release(3);
    t.assert_exited(3);
  });
}

TEST_CASE("A lock should block an earlier writer and raise the time", "[lock]") {
  repeat ([] {
    ConditionLock lock;
    lock.write(2);
    spawn t([&lock] {
      return lock.write(1);
    });
    t.assert_alive();
    lock.release(3);
    t.assert_exited(3);
  });
}

TEST_CASE("A lock should block a later reader", "[lock]") {
  repeat ([] {
    ConditionLock lock;
    lock.write(1);
    spawn t([&lock] {
      lock.read(2);
      return 0;
    });
    t.assert_alive();
    lock.release(2);
    t.assert_exited(0);
  });
}

TEST_CASE("A lock should block a later reader and raise the time", "[lock]") {
  repeat ([] {
    ConditionLock lock;
    lock.write(1);
    spawn t([&lock] {
      lock.read(3);
      return 0;
    });
    t.assert_alive();
    lock.release(2);
    t.assert_exited(0);
    REQUIRE(lock.write(2) == 3);
  });
}

TEST_CASE("A lock should an earlier reader", "[lock]") {
  repeat ([] {
    ConditionLock lock;
    lock.write(1);
    spawn t([&lock] {
      lock.read(1);
      return 0;
    });
    t.assert_exited(0);
  });
}
