#define CATCH_CONFIG_MAIN

#include <cstdint>
#include <iostream>
#include <map>
#include <unordered_map>
#include <vector>

#include "CppUnorderedMapOfMap.hpp"
#include "catch.hpp"
#include "table.hpp"

using std::cout;
using std::endl;
using std::initializer_list;
using std::vector;

void write(Table &table, uint32_t time, initializer_list<Row> rows) {
  table.write(time, vector<Row> (rows));
}

void expect(Table &table, uint32_t time, initializer_list<int> keys, initializer_list<Value> expected) {
  REQUIRE(table.read(time, vector<int> (keys)) == vector<Value> (expected));
}

void expectMoneyConserved(const CppUnorderedMapOfMap &table) {
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

TEST_CASE("A table should read 0 for any key", "[table]") {
  CppUnorderedMapOfMap table;
  expect(table, 0, {0}, {Value(0, 0)});
  expect(table, 1, {0}, {Value(0, 0)});
}

TEST_CASE("A table should read what was put", "[table]") {
  CppUnorderedMapOfMap table;
  write(table, 0, {Row(0, 1)});
  expect(table, 1, {0}, {Value(1, 1)});
}

TEST_CASE("A table should read and write batches", "[table]") {
  CppUnorderedMapOfMap table;
  write(table, 0, {Row(0, 1), Row(1, 2)});
  expect(table, 1, {0, 1}, {Value(1, 1), Value(2, 1)});
}

TEST_CASE("A table should reject a stale write", "[table]") {
  CppUnorderedMapOfMap table;
  write(table, 0, {Row(0, 1)});
  REQUIRE_THROWS_AS(write(table, 0, {Row(0,2)}), stale_exception);
  expect(table, 1, {0}, {Value(1,1)});
}

TEST_CASE("A table should preserve the money supply running serially", "[table]") {
  CppUnorderedMapOfMap table;
  broker(table, 1000);
  expectMoneyConserved(table);
}
