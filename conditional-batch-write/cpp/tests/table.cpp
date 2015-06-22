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

#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "CppUnorderedMapOfMap.hpp"
#include "CppVector.hpp"
#include "StdConditionLock.hpp"
#include "TbbConditionLock.hpp"
#include "catch.hpp"
#include "lock.hpp"
#include "table.hpp"

using std::function;
using std::initializer_list;
using std::thread;
using std::unique_ptr;
using std::vector;

void write(Table &table, uint32_t time, initializer_list<Row> rows) {
  table.write(time, vector<Row> (rows));
}

void expect(Table &table, uint32_t time, initializer_list<int> keys, initializer_list<Value> expected) {
  REQUIRE(table.read(time, vector<int> (keys)) == vector<Value> (expected));
}

void expectMoneyConserved(const Table &table) {
  std::map<uint32_t, vector<Row>> history;
  for (auto c : table.scan())
    history[c.t].push_back(Row(c.k, c.v));
  std::unordered_map<int, int> tracker;
  for (auto e : history) {
    for (auto r : e.second)
      tracker[r.k] = r.v;
    int sum = 0;
    for (auto e : tracker)
      sum += e.second;
    REQUIRE(sum == 0);
  }
}

void table_behaviors(const function<Table*(Params &)> &new_table, bool parallel) {

  Params params("unknown", 8, 8, 100, 8, 3200);

  SECTION("A table should read 0 for any key", "[table]") {
    unique_ptr<Table> table (new_table(params));
    expect(*table, 0, {0}, {Value(0, 0)});
    expect(*table, 1, {0}, {Value(0, 0)});
  }

  SECTION("A table should read what was put", "[table]") {
    unique_ptr<Table> table (new_table(params));
    write(*table, 0, {Row(0, 1)});
    expect(*table, 1, {0}, {Value(1, 1)});
  }

  SECTION("A table should read and write batches", "[table]") {
    unique_ptr<Table> table (new_table(params));
    write(*table, 0, {Row(0, 1), Row(1, 2)});
    expect(*table, 1, {0, 1}, {Value(1, 1), Value(2, 1)});
  }

  SECTION("A table should reject a stale write", "[table]") {
    unique_ptr<Table> table (new_table(params));
    write(*table, 0, {Row(0, 1)});
    REQUIRE_THROWS_AS(write(*table, 0, {Row(0,2)}), stale_exception);
    expect(*table, 1, {0}, {Value(1,1)});
  }

  SECTION("A table should preserve the money supply running serially", "[table]") {
    unique_ptr<Table> table (new_table(params));
    broker(*table, params);
    expectMoneyConserved(*table);
  }

  if (parallel) {
    SECTION("A table should preserve the money supply running in parallel", "[table]") {
      unique_ptr<Table> table_ptr (new_table(params));
      auto &table = *table_ptr;
      vector<thread> brokers;
      for (int i = 0; i < params.nbrokers; ++i) {
        brokers.push_back(thread([&] {
          broker(table, params);
        }));
      }
      for (auto &b: brokers)
        b.join();
      expectMoneyConserved(table);
    }
  }
}

TEST_CASE ("The CppUnorderedMapOfMap should work", "[tables]") {
  table_behaviors([] (Params &params) {
    return new TableFromShard<CppUnorderedMapOfMap>();
  }, false);
}

TEST_CASE ("The CppVector should work", "[tables]") {
  table_behaviors([] (Params &params) {
    auto copy = params;
    copy.nshards = params.nlocks;
    return new ShardedTable<LockSpace<StdConditionLock>, TbbMutexShard<CppVector>>(copy);
  }, true);
}

TEST_CASE ("The StdLockAndTable should work", "[tables]") {
  table_behaviors([] (Params &params) {
    return new ShardedTable<LockSpace<StdConditionLock>, StdMutexShard<CppUnorderedMapOfMap>>(params);
  }, true);
}

TEST_CASE ("The StdLockTbbTable should work", "[tables]") {
  table_behaviors([] (Params &params) {
    return new ShardedTable<LockSpace<StdConditionLock>, TbbMutexShard<CppUnorderedMapOfMap>>(params);
  }, true);
}

TEST_CASE ("The TbbLockStdTable should work", "[tables]") {
  table_behaviors([] (Params &params) {
    return new ShardedTable<LockSpace<TbbConditionLock>, StdMutexShard<CppUnorderedMapOfMap>>(params);
  }, true);
}

TEST_CASE ("The TbbLockAndTable should work", "[tables]") {
  table_behaviors([] (Params &params) {
    return new ShardedTable<LockSpace<TbbConditionLock>, TbbMutexShard<CppUnorderedMapOfMap>>(params);
  }, true);
}
