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

#ifndef CPP_VECTOR_HPP
#define CPP_VECTOR_HPP

#include <vector>

#include "table.hpp"

class CppVector: public Shard {

  public:

    void read (uint32_t t, int k, Value &v) const {
      for (auto i = cells.rbegin(); i != cells.rend(); ++i)
        if (i->k == k && i->t <= t) {
          v = Value (i->v, i->t);
          return;
        }
      v = Value(0, 0);
    }

    uint32_t prepare(int k) const {
      for (auto i = cells.rbegin(); i != cells.rend(); ++i)
        if (i->k == k)
          return i->t;
      return 0;
    }

    void commit(uint32_t t, int k, int v) {
      cells.emplace_back(k, v, t);
    }

    void scan (uint32_t t, std::vector<Cell> &cs) const {
      for (auto c: cells)
        if (c.t <= t)
          cs.emplace_back(c.k, c.v, c.t);
    }

  private:

    std::vector<Cell> cells;
};

#endif // CPP_VECTOR_HPP