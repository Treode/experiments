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

#include <ctime>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "CppUnorderedMapOfMap.hpp"
#include "StdConditionLock.hpp"
#include "TbbConditionLock.hpp"
#include "lock.hpp"
#include "table.hpp"

using std::condition_variable;
using std::cout;
using std::endl;
using std::function;
using std::lock_guard;
using std::mutex;
using std::ostream;
using std::string;
using std::thread;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

struct PerfResult {

  string name;
  string platform;
  size_t nshards;
  size_t nbrokers;
  double result;

  PerfResult(const string &_name, const Params &params, double _result):
    name(_name),
    platform(params.platform),
    nshards(params.nshards),
    nbrokers(params.nbrokers),
    result(_result)
  {}
};

ostream &operator<<(ostream &os, const PerfResult v) {
  os << v.name
     << ", c++, " << v.platform
     << ", " << v.nshards
     << ", " << v.nbrokers
     << ", " << v.result;
  return os;
}

class CountDownLatch {

  public:

    CountDownLatch(unsigned _count): count(_count) {}

    void signal() {
      unsigned _count;
      {
        lock_guard<mutex> acqn(lock);
        if (count > 0)
          --count;
        _count = count;
      }
      if (_count == 0)
        cond.notify_all();
    }

    void wait() {
      unique_lock<mutex> acqn(lock);
      while (count > 0)
        cond.wait(acqn);
    }

  private:
    mutex lock;
    condition_variable cond;
    unsigned count;
};

clock_t serial_brokers(const function<Table*(void)> &new_table, const Params &params) {
  unique_ptr<Table> table (new_table());
  auto start = clock();
  for (int i = 0; i < params.nbrokers; ++i)
    broker(*table, params);
  auto end = clock();
  return end - start;
}

clock_t parallel_brokers(const function<Table*(void)> &new_table, const Params &params) {
  unique_ptr<Table> table_ptr (new_table());
  auto &table = *table_ptr;
  vector<thread> brokers;
  CountDownLatch ready(params.nbrokers);
  CountDownLatch gate(1);
  CountDownLatch finished(params.nbrokers);
  for (int i = 0; i < params.nbrokers; ++i) {
    brokers.push_back(thread([&] {
      ready.signal();
      gate.wait();
      broker(table, params);
      finished.signal();
    }));
  }
  ready.wait();
  auto start = clock();
  gate.signal();
  finished.wait();
  auto end = clock();
  for (auto &b: brokers)
    b.join();
  return end - start;
}

void perf(
  const function<Table*(void)> &new_table,
  const string &name,
  bool parallel,
  const Params &params,
  vector<PerfResult> &results
) {

  unsigned nhits = 5;
  unsigned ntrials = 2000;
  unsigned nclocks = 60 * CLOCKS_PER_SEC;
  double tolerance = 0.01;
  double ops = params.ntransfers;

  double sum = 0.0;

  cout << name << ", " << params << endl;

  unsigned trial = 0;
  unsigned hits = 0;
  auto limit = clock() + nclocks;
  while (trial < ntrials && hits < nhits && clock() < limit) {
    double us = parallel ?
      parallel_brokers(new_table, params) :
      serial_brokers(new_table, params);
    double x = ops / us * (CLOCKS_PER_SEC / 1000);
    sum += x;
    ++trial;
    double mean = sum / (double)(trial);
    double dev = std::abs (x - mean) / mean;
    if (dev <= tolerance) {
      cout << trial << " " << x << " ops/ms (" << mean << ")" << endl;
      ++hits;
    }
  }
  double mean = sum / (double)(trial);
  results.push_back (PerfResult(name, params, mean));
}

int main() {

  string platform("unknown");

  // Powers of 2, from 1 to availableProcessors (or next power of 2).
  vector<unsigned> shards;
  unsigned limit = thread::hardware_concurrency() * 2 - 1;
  for (unsigned i = 1; i < limit; i = i << 1)
    shards.push_back(i);

  vector<unsigned> brokers;
  for (unsigned i = 1; i <= 64; i = i << 1)
    brokers.push_back(i);

  vector<PerfResult> results;

  {
    Params params(platform, 128, 1, 100, 1, 6400);

    perf([] {
      return new CppUnorderedMapOfMap();
    }, "CppUnorderedMapOfMap", false, params, results);
  }

  for (auto nshards: shards) {
    for (auto nbrokers: brokers) {

      Params params(platform, 128, nshards, 100, nbrokers, 6400);

      perf([=, &params] {
        return new ShardedTable<LockSpace<StdConditionLock>, StdMutexShard<CppUnorderedMapOfMap>>(params);
      }, "StdLockAndTable", true, params, results);

      perf([=, &params] {
        return new ShardedTable<LockSpace<StdConditionLock>, TbbMutexShard<CppUnorderedMapOfMap>>(params);
      }, "StdLockTbbTable", true, params, results);

      perf([=, &params] {
        return new ShardedTable<LockSpace<TbbConditionLock>, StdMutexShard<CppUnorderedMapOfMap>>(params);
      }, "TbbLockStdTable", true, params, results);

      perf([=, &params] {
        return new ShardedTable<LockSpace<TbbConditionLock>, TbbMutexShard<CppUnorderedMapOfMap>>(params);
      }, "TbbLockAndTable", true, params, results);
    }
  }

  for (auto &r: results)
    cout << r << endl;
}
