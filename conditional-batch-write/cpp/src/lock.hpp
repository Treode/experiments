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

#ifndef LOCK_HPP
#define LOCK_HPP

#include <algorithm>
#include <vector>

#include "table.hpp"

class Lock {

  public:

    virtual uint32_t time() = 0;

    virtual void read(uint32_t time) = 0;

    virtual uint32_t write(uint32_t time) = 0;

    virtual void release(uint32_t time) = 0;
};

// L is a Lock.
template <typename L>
class LockSpace {

  public:

    LockSpace(Params &params):
      size(params.nlocks),
      mask(params.nlocks - 1),
      clock(0),
      locks(params.nlocks)
    {}

    uint32_t time() {
      return clock.load();
    }

    void read(uint32_t t, size_t n, const int *ks) {
      raise(t);
      int is[n];
      auto end = idxs(n, ks, is);
      for (auto i = is; i < end; ++i)
        locks[*i].read(t);
    }

    void read(uint32_t t, std::vector<int> ks) {
      read(t, ks.size(), ks.data());
    }

    uint32_t write(uint32_t t, size_t n, const Row *rs) {
      raise(t);
      int is[n];
      auto end = idxs(n, rs, is);
      uint32_t max = 0;
      for(auto i = is; i < end; ++i) {
        auto t2 = locks[*i].write(t);
        if (max < t2)
          max = t2;
      }
      return max;
    }

    uint32_t write(uint32_t t, std::vector<Row> rs) {
      return write(t, rs.size(), rs.data());
    }

    void release(uint32_t t, size_t n, const Row *rs) {
      raise(t);
      int is[n];
      auto end = idxs(n, rs, is);
      for (auto i = is; i < end; ++i)
        locks[*i].release(t);
    }

    void release(uint32_t t, std::vector<Row> rs) {
      release(t, rs.size(), rs.data());
    }

    void scan(uint32_t t) {
      raise(t);
      for (size_t i = 0; i < size; ++i)
        locks[i].read(t);
    }

  private:

    const size_t size;
    const size_t mask;
    std::atomic<uint32_t> clock;
    std::vector<L> locks;

    void raise(uint32_t time) {
      auto now = clock.load();
      while (now < time && !clock.compare_exchange_weak(now, time));
    }

    int *idxs(size_t n, const int *ks, int *is) {
      for (size_t i = 0; i < n; ++i)
        is[i] = ks[i] & mask;
      std::sort(is, is + n);
      return std::unique(is, is + n);
    }

    int *idxs(size_t n, const Row *rs, int *is) {
      for (size_t i = 0; i < n; ++i)
        is[i] = rs[i].k & mask;
      std::sort(is, is + n);
      return std::unique(is, is + n);
    }
};

#endif // LOCK_HPP
