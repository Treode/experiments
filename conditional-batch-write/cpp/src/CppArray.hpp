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

#ifndef CPP_ARRAY_HPP
#define CPP_ARRAY_HPP

#include <cassert>

#include "lock.hpp"
#include "table.hpp"

#define CPP_ARRAY_SIZE 1000

class CppArray: public LockingShard {

  public:

    CppArray(): head(CPP_ARRAY_SIZE) {}

    uint32_t time() const {
      return lock.time();
    }

    void read (uint32_t t, int k, Value &v) const {
      lock.read_acquire(t);
      unsigned h = head;
      lock.read_release();
      _read(h, t, k, v);
    }

    void prepare(uint32_t ct, int k, uint32_t &vt, uint32_t &lt) const {
      auto now = lock.write_acquire(ct);
      unsigned h = head;
      lock.write_release();
      if (lt < now)
        lt = now;
      _prepare(h, k, vt);
    }

    void prepare(uint32_t ct, int k1, int k2, uint32_t &vt, uint32_t &lt) const {
      auto now = lock.write_acquire(ct);
      unsigned h = head;
      lock.write_release();
      if (lt < now)
        lt = now;
      _prepare(h, k1, vt);
      _prepare(h, k2, vt);
    }

    void commit(uint32_t t, int k, int v) {
      lock.release_acquire(t);
      assert (head > 0);
      cells[head-1] = Cell(k, v, t);
      --head;
      lock.release_release();
    }

    void commit(uint32_t t, int k1, int v1, int k2, int v2) {
      lock.release_acquire(t);
      assert (head > 1);
      cells[head-1] = Cell(k1, v1, t);
      cells[head-2] = Cell(k2, v2, t);
      head -= 2;
      lock.release_release();
    }

    void abort() {
      lock.release(0);
    }

    void scan (uint32_t t, std::vector<Cell> &cs) const {
      lock.read_acquire(t);
      unsigned h = head;
      lock.read_release();
      for (unsigned i = h; i < CPP_ARRAY_SIZE; ++i) {
        auto &c = cells[i];
        if (c.t <= t)
          cs.emplace_back(c.k, c.v, c.t);
      }
    }

  private:

    mutable TbbLock lock;
    unsigned head;
    Cell cells[CPP_ARRAY_SIZE];

    void _read (unsigned head, uint32_t t, int k, Value &v) const {
      for (unsigned i = head; i < CPP_ARRAY_SIZE; ++i) {
        auto &c = cells[i];
        if (c.k == k && c.t <= t) {
          v = Value (c.v, c.t);
          return;
        }
      }
      v = Value(0, 0);
    }

    void _prepare(unsigned head, int k, uint32_t &vt) const {
      for (unsigned i = head; i < CPP_ARRAY_SIZE; ++i) {
        auto &c = cells[i];
        if (c.k == k) {
          if (vt < c.t)
            vt = c.t;
          return;
        }
      }
    }
};

#endif // CPP_ARRAY_HPP
