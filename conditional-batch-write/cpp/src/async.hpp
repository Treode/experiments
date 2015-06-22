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

#ifndef ASYNC_HPP
#define ASYNC_HPP

#include <chrono>
#include <deque>
#include <functional>
#include <mutex>

#include "table.hpp"
#include "tbb/task.h"

class Fiber {

  public:

    Fiber(): engaged(false) {}

    template <typename F>
    tbb::task *enqueue(tbb::task *current, F f);

    tbb::task *dequeue(tbb::task *current) {
      auto t = dequeue();
      if (t != nullptr)
        current->spawn(*t);
      return nullptr;
    }

  private:

    tbb::task *enqueue(tbb::task *t) {
      std::lock_guard<std::mutex> acqn(lock);
      if (!engaged) {
        engaged = true;
        return t;
      } else {
        tasks.push_back(t);
        return nullptr;
      }
    }

    tbb::task *dequeue() {
      std::lock_guard<std::mutex> acqn(lock);
      if (tasks.empty()) {
        engaged = false;
        return nullptr;
      } else {
        auto t = tasks.front();
        tasks.pop_front();
        return t;
      }
    }

    std::mutex lock;
    std::deque<tbb::task*> tasks;
    bool engaged;
};

template <typename F>
class FiberTask: public tbb::task {

  public:

    FiberTask(Fiber &_fiber, F _func): fiber (_fiber), func(_func) {}

    task *execute() {
      func();
      return fiber.dequeue(this);
    }

  private:

    Fiber &fiber;
    F func;
};

template <typename F>
tbb::task *Fiber::enqueue(tbb::task *current, F f) {
  return enqueue(new(current->allocate_child()) FiberTask<F>(*this, f));
}

class AsyncTable {

  public:

    virtual uint32_t time() = 0;

    virtual tbb::task* read(uint32_t t, size_t n, const int *ks, Value *vs, tbb::task *current) = 0;

    virtual tbb::task *write(uint32_t t, size_t n, const Row *rs, tbb::task *current) = 0;
};

// T is a Table
template <typename T>
class FiberizedTable: public AsyncTable {

  public:

    uint32_t time() {
      return table.time();
    }

    tbb::task* read(uint32_t t, size_t n, const int *ks, Value *vs, tbb::task *current) {
      return fiber.enqueue(current, [=] {
        table.read(t, n, ks, vs);
      });
    }

    tbb::task *write(uint32_t t, size_t n, const Row *rs, tbb::task *current) {
      return fiber.enqueue(current, [=] {
        try {
          table.write(t, n, rs);
        } catch (stale_exception e) {
          // ignored
        }
      });
    }

  private:

    T table;
    Fiber fiber;
};

std::chrono::high_resolution_clock::duration
async_brokers(const std::function<AsyncTable*(void)> &new_table, const Params &params);

#endif // ASYNC_HPP
