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

#include <ostream>
#include <random>

#include "CppUnorderedMapOfMap.hpp"
#include "table.hpp"

using std::ostream;
using std::vector;

std::ostream &operator<<(std::ostream &os, const Params p) {
  os << "platform: " << p.platform
     << ", nlocks: " << p.nlocks
     << ", nshards: " << p.nshards
     << ", naccounts: " << p.naccounts
     << ", nbrokers: " << p.nbrokers
     << ", ntransfers: " << p.ntransfers;
  return os;
}

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

// Simulate doing other work.
unsigned fib(unsigned n) {
  if (n < 2)
    return 1;
  return fib(n-1) + fib(n-2);
}

unsigned broker(Table &table, const Params &params) {

  std::default_random_engine reng;
  std::uniform_int_distribution<int> racct(0, 100);
  std::uniform_int_distribution<int> ramt(0, 1000);
  unsigned sum = 0;

  auto count = params.ntransfers / params.nbrokers;
  for (unsigned i = 0; i < count; ++i) {
    int a1 = racct(reng);
    int a2;
    while ((a2 = racct (reng)) == a1);
    auto n = ramt(reng);

    //sum += fib(10); // read request from network
    auto rt = table.time();
    int ks[] = {a1, a2};
    Value vs[2];
    table.read(rt, 2, ks, vs);

    //sum += fib(10); // processing
    try {
      Row rs[] = {Row(a1, vs[0].v - n), Row(a2, vs[1].v + n)};
      table.write(rt, 2, rs);
    } catch (stale_exception e) {
      // ignored
    }

    //sum += fib(10); // write response to network
  }

  return sum;
}
