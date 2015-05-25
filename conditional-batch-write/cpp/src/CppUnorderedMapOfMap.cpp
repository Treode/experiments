#include <iostream>
#include <vector>

#include "CppUnorderedMapOfMap.hpp"

using std::cout;
using std::endl;
using std::initializer_list;
using std::ostream;
using std::vector;

inline Value CppUnorderedMapOfMap::read(uint32_t t, int k) const {
  auto i = table.find(k);
  if (i == table.end())
    return Value(0, 0);
  auto j = i->second.lower_bound(UINT32_MAX - t);
  if (j == i->second.end())
    return Value(0, 0);
  return Value(j->second, UINT32_MAX - j->first);
}

vector<Value> CppUnorderedMapOfMap::read(uint32_t t, initializer_list<int> ks) {
  raise(t);
  vector<Value> vs;
  for (auto k: ks)
    vs.push_back(read(t, k));
  return vs;
}

inline int CppUnorderedMapOfMap::prepare(const Row &r) const {
  auto i = table.find(r.k);
  if (i == table.end())
    return 0;
  return UINT32_MAX - i->second.begin()->first;
}

inline void CppUnorderedMapOfMap::prepare(uint32_t t, initializer_list<Row> rs) const {
  uint32_t max = 0;
  for (auto r: rs) {
    auto t2 = prepare(r);
    if (max < t2)
      max = t2;
  }
  if (max > t)
    throw stale_exception (t, max);
}

inline void CppUnorderedMapOfMap::commit(uint32_t t, const Row &r) {
  table[r.k][UINT32_MAX - t] = r.v;
}

inline uint32_t CppUnorderedMapOfMap::commit(initializer_list<Row> rs) {
  auto t = ++clock;
  for (auto r : rs)
    commit (clock, r);
  return clock;
}

uint32_t CppUnorderedMapOfMap::write(uint32_t t, initializer_list<Row> rs) {
  raise(t);
  prepare(t, rs);
  return commit(rs);
}

vector<Cell> CppUnorderedMapOfMap::scan() const {
  vector<Cell> cs;
  for (auto kvs: table)
    for (auto v: kvs.second)
      cs.push_back(Cell (kvs.first, v.second, UINT32_MAX - v.first));
  return cs;
}
