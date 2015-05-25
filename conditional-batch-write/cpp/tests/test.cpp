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

void expect(const vector<Value> &actual, initializer_list<Value> expected) {
  REQUIRE(actual == vector<Value> (expected));
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
  expect(table.read(0, {0}), {Value(0, 0)});
  expect(table.read(1, {0}), {Value(0, 0)});
}

TEST_CASE("A table should read what was put", "[table]") {
  CppUnorderedMapOfMap table;
  table.write(0, {Row(0, 1)});
  expect(table.read(1, {0}), {Value(1, 1)});
}

TEST_CASE("A table should read and write batches", "[table]") {
  CppUnorderedMapOfMap table;
  table.write(0, {Row(0, 1), Row(1, 2)});
  expect(table.read(1, {0, 1}), {Value(1, 1), Value(2, 1)});
}

TEST_CASE("A table should reject a stale write", "[table]") {
  CppUnorderedMapOfMap table;
  table.write(0, {Row(0, 1)});
  REQUIRE_THROWS_AS(table.write(0, {Row(0,2)}), stale_exception);
  expect(table.read(1, {0}), {Value(1,1)});
}

TEST_CASE("A table should preserve the money supply running serially", "[table]") {
  CppUnorderedMapOfMap table;
  broker(table);
  expectMoneyConserved(table);
}
