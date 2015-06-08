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

class CppUnorderedMapOfMap: public Table, Shard {

  public:

    uint32_t time() const {
      return clock;
    }

    Value read (uint32_t t, int k) const;

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const;

    uint32_t prepare(const Row &r) const;

    void commit(uint32_t t, const Row &r);

    uint32_t write(uint32_t t, size_t n, const Row *rs);

    std::vector<Cell> scan (uint32_t t) const;

    std::vector<Cell> scan() const;

  private:

    mutable uint32_t clock = 0;

    std::unordered_map<int, std::map<uint32_t, int>> table;

    void raise(uint32_t t) const {
      if (clock < t)
        clock = t;
    }

    void prepare(uint32_t t, size_t n, const Row *rs) const;

    uint32_t commit(size_t n, const Row *rs);

    std::vector<Cell> _scan (uint32_t t) const;
};

#endif // CPP_UNORDERED_MAP_OF_MAP_HPP
