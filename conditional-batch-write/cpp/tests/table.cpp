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

#include "ConditionLock.hpp"
#include "CppUnorderedMapOfMap.hpp"
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

void table_behaviors(const function<Table*(void)> &new_table, bool parallel) {

  size_t nbrokers = 8;
  size_t ntransfers = 1000;

  SECTION("A table should read 0 for any key", "[table]") {
    unique_ptr<Table> table (new_table());
    expect(*table, 0, {0}, {Value(0, 0)});
    expect(*table, 1, {0}, {Value(0, 0)});
  }

  SECTION("A table should read what was put", "[table]") {
    unique_ptr<Table> table (new_table());
    write(*table, 0, {Row(0, 1)});
    expect(*table, 1, {0}, {Value(1, 1)});
  }

  SECTION("A table should read and write batches", "[table]") {
    unique_ptr<Table> table (new_table());
    write(*table, 0, {Row(0, 1), Row(1, 2)});
    expect(*table, 1, {0, 1}, {Value(1, 1), Value(2, 1)});
  }

  SECTION("A table should reject a stale write", "[table]") {
    unique_ptr<Table> table (new_table());
    write(*table, 0, {Row(0, 1)});
    REQUIRE_THROWS_AS(write(*table, 0, {Row(0,2)}), stale_exception);
    expect(*table, 1, {0}, {Value(1,1)});
  }

  SECTION("A table should preserve the money supply running serially", "[table]") {
    unique_ptr<Table> table (new_table());
    broker(*table, ntransfers);
    expectMoneyConserved(*table);
  }

  if (parallel) {
    SECTION("A table should preserve the money supply running in parallel", "[table]") {
      unique_ptr<Table> table_ptr (new_table());
      auto &table = *table_ptr;
      vector<thread> brokers;
      for (int i = 0; i < nbrokers; ++i) {
        brokers.push_back(thread([&table, ntransfers] {
          broker(table, ntransfers);
        }));
      }
      for (auto &b: brokers)
        b.join();
      expectMoneyConserved(table);
    }
  }
}

TEST_CASE ("The CppUnorderedMapOfMap should work", "[tables]") {
  table_behaviors([] {
    return new CppUnorderedMapOfMap();
  }, false);
}

TEST_CASE ("The ShardedTable should work", "[tables]") {
  table_behaviors([] {
    return new ShardedTable<LockSpace<ConditionLock>, MutexShard<CppUnorderedMapOfMap>>(128, 16);
  }, true);
}
