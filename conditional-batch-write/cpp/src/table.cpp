#include <ostream>
#include <random>

#include "CppUnorderedMapOfMap.hpp"
#include "table.hpp"

using std::ostream;
using std::vector;

ostream &operator<<(ostream &os, const Value &v) {
  os << "Value(" << v.v << ", " << v.t << ")";
  return os;
}

bool operator==(const Value &x, const Value &y) {
  return x.v == y.v && x.t == y.t;
}


ostream &operator<<(ostream &os, const Row &r) {
  os << "Row(" << r.k << ", " << r.v << ")";
  return os;
}

bool operator==(const Row &x, const Row &y) {
  return x.k == y.k && x.v == y.v;
}

ostream &operator<<(ostream &os, const Cell &c) {
  os << "Cell(" << c.k << ", " << c.v << ", " << c.t << ")";
  return os;
}

bool operator==(const Cell &x, const Cell &y) {
  return x.k == y.k && x.v == y.v && x.t == y.t;
}

ostream &operator<<(ostream &os, const Table &table) {
  for (auto c : table.scan())
    os << c;
  return os;
}

void broker(Table &table, unsigned ntransfers) {

  std::default_random_engine reng;
  std::uniform_int_distribution<int> racct(0, 100);
  std::uniform_int_distribution<int> ramt(0, 1000);

  for (unsigned i = 0; i < ntransfers; ++i) {
    int a1 = racct(reng);
    int a2;
    while ((a2 = racct (reng)) == a1);
    auto n = ramt(reng);

    auto rt = table.time();
    int ks[] = {a1, a2};
    Value vs[2];
    table.read(rt, 2, ks, vs);
    try {
      Row rs[] = {Row(a1, vs[0].v - n), Row(a2, vs[1].v + n)};
      table.write(rt, 2, rs);
    } catch (stale_exception e) {
      // ignored
    }
  }
}
