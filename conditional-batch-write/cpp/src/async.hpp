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
#include "tbb/spin_mutex.h"
#include "tbb/task.h"

class Fiber {

  public:

    virtual ~Fiber() = default;

    template <typename F>
    tbb::task *enqueue(tbb::task *current, F f);

    tbb::task *dequeue(tbb::task *current) {
      auto t = _dequeue();
      if (t != nullptr)
        current->spawn(*t);
      return nullptr;
    }

  private:

    virtual tbb::task *_enqueue(tbb::task *task) =  0;

    virtual tbb::task *_dequeue() = 0;
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
  return _enqueue(new(current->allocate_child()) FiberTask<F>(*this, f));
}

class StdFiber: public Fiber {

  public:

    StdFiber(): engaged(false) {}

  private:

    tbb::task *_enqueue(tbb::task *t) {
      std::unique_lock<std::mutex> acqn(lock);
      if (!engaged) {
        engaged = true;
        return t;
      } else {
        tasks.push_back(t);
        return nullptr;
      }
    }

    tbb::task *_dequeue() {
      std::unique_lock<std::mutex> acqn(lock);
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

class TbbFiber: public Fiber {

  public:

    TbbFiber(): engaged(false) {}

  private:

    tbb::task *_enqueue(tbb::task *t) {
      tbb::spin_mutex::scoped_lock acqn(lock);
      if (!engaged) {
        engaged = true;
        return t;
      } else {
        tasks.push_back(t);
        return nullptr;
      }
    }

    tbb::task *_dequeue() {
      tbb::spin_mutex::scoped_lock acqn(lock);
      if (tasks.empty()) {
        engaged = false;
        return nullptr;
      } else {
        auto t = tasks.front();
        tasks.pop_front();
        return t;
      }
    }

    tbb::spin_mutex lock;
    std::deque<tbb::task*> tasks;
    bool engaged;
};

class AsyncTable {

  public:

    virtual uint32_t time() = 0;

    virtual tbb::task* read(uint32_t t, size_t n, const int *ks, Value *vs, tbb::task *current) = 0;

    virtual tbb::task *write(uint32_t t, size_t n, const Row *rs, tbb::task *current) = 0;
};

// T is a Table, F is a Fiber
template <typename T, typename F>
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
    F fiber;
};

std::chrono::high_resolution_clock::duration
async_brokers(const std::function<AsyncTable*(void)> &new_table, const Params &params);

#endif // ASYNC_HPP
