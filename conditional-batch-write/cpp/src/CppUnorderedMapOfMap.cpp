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

void CppUnorderedMapOfMap::read(uint32_t t, size_t n, const int *ks, Value *vs) {
  raise(t);
  for (size_t i = 0; i < n; ++i)
    vs[i] = read(t, ks[i]);
}

inline int CppUnorderedMapOfMap::prepare(const Row &r) const {
  auto i = table.find(r.k);
  if (i == table.end())
    return 0;
  return UINT32_MAX - i->second.begin()->first;
}

inline void CppUnorderedMapOfMap::prepare(uint32_t t, size_t n, const Row *rs) const {
  uint32_t max = 0;
  for (size_t i = 0; i < n; ++i) {
    auto t2 = prepare(rs[i]);
    if (max < t2)
      max = t2;
  }
  if (max > t)
    throw stale_exception (t, max);
}

inline void CppUnorderedMapOfMap::commit(uint32_t t, const Row &r) {
  table[r.k][UINT32_MAX - t] = r.v;
}

inline uint32_t CppUnorderedMapOfMap::commit(size_t n, const Row *rs) {
  auto t = ++clock;
  for (size_t i = 0; i < n; ++i)
    commit (clock, rs[i]);
  return clock;
}

uint32_t CppUnorderedMapOfMap::write(uint32_t t, size_t n, const Row *rs) {
  raise(t);
  prepare(t, n, rs);
  return commit(n, rs);
}

vector<Cell> CppUnorderedMapOfMap::scan() const {
  vector<Cell> cs;
  for (auto kvs: table)
    for (auto v: kvs.second)
      cs.push_back(Cell (kvs.first, v.second, UINT32_MAX - v.first));
  return cs;
}
