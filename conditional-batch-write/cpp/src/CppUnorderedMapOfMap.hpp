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

#ifndef CPP_UNORDERED_MAP_OF_MAP_HPP
#define CPP_UNORDERED_MAP_OF_MAP_HPP

#include <map>
#include <unordered_map>
#include <vector>

#include "table.hpp"

class CppUnorderedMapOfMap: public Shard {

  public:

    void read (uint32_t t, int k, Value &v) const {
      auto i = table.find(k);
      if (i == table.end()) {
        v = Value(0, 0);
        return;
      }
      auto j = i->second.lower_bound(UINT32_MAX - t);
      if (j == i->second.end()) {
        v = Value(0, 0);
        return;
      }
      v = Value(j->second, UINT32_MAX - j->first);
    }

    uint32_t prepare(int k) const {
      auto i = table.find(k);
      if (i == table.end())
        return 0;
      return UINT32_MAX - i->second.begin()->first;
    }

    void commit(uint32_t t, int k, int v) {
      table[k][UINT32_MAX - t] = v;
    }

    void scan (uint32_t t, std::vector<Cell> &cs) const {
      for (auto kvs: table) {
        for (auto v: kvs.second) {
          auto t2 = UINT32_MAX - v.first;
          if (t2 <= t)
            cs.emplace_back(kvs.first, v.second, t2);
        }
      }
    }

  private:

    std::unordered_map<int, std::map<uint32_t, int>> table;
};

#endif // CPP_UNORDERED_MAP_OF_MAP_HPP
