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

#ifndef TABLE_HPP
#define TABLE_HPP

#include <cstdint>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <vector>

struct Value {

  int v;
  uint32_t t;

  Value() {}

  constexpr Value(int _v, uint32_t _t): v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Value &v);

bool operator==(const Value &x, const Value &y);

inline bool operator!=(const Value &x, const Value &y) {
  return !(x == y);
}

struct Row {

  int k, v;

  constexpr Row(int _k, int _v): k(_k), v(_v) {}
};

std::ostream &operator<<(std::ostream &os, const Row &r);

bool operator==(const Row &x, const Row &y);

inline bool operator!=(const Row &x, const Row &y) {
  return !(x == y);
}

struct Cell {

  int k, v;
  uint32_t t;

  constexpr Cell(int _k, int _v, uint32_t _t): k(_k), v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Cell &c);

bool operator==(const Cell &x, const Cell &y);

inline bool operator!=(const Cell &x, const Cell &y) {
  return !(x == y);
}

class stale_exception: public std::runtime_error {

  public:

    uint32_t cond, max;

    stale_exception (uint32_t _cond, uint32_t _max):
      runtime_error ("stale"),
      cond (_cond),
      max (_max)
    {}
};

class Table {

  public:

    virtual ~Table() = default;

    virtual uint32_t time() const = 0;

    virtual void read(uint32_t t, size_t n, const int *ks, Value *vs) const = 0;

    virtual uint32_t write(uint32_t t, size_t n, const Row *rs) = 0;

    virtual std::vector<Cell> scan() const =  0;

    std::vector<Value> read(uint32_t t, const std::vector<int> &ks) const {
      auto vs = std::vector<Value>(ks.size());
      read(t, ks.size(), ks.data(), vs.data());
      return vs;
    }

    uint32_t write(uint32_t t, const std::vector<Row> &rs) {
      return write(t, rs.size(), rs.data());
    }
};

std::ostream &operator<<(std::ostream &os, const Table &table);

void broker(Table &table, unsigned ntransfers);

class Shard {

  public:

    virtual ~Shard() = default;

    virtual Value read(uint32_t t, int k) const = 0;

    virtual uint32_t prepare(const Row &r) const = 0;

    virtual void commit(uint32_t t, const Row &r) = 0;

    virtual std::vector<Cell> scan(uint32_t t) const = 0;
};

// S is a shard
template <typename S>
class MutexShard: public Shard {

  public:

    Value read(uint32_t t, int k) const {
      std::lock_guard<std::mutex> acqn(lock);
      return shard.read(t, k);
    }

    uint32_t prepare(const Row &r) const {
      std::lock_guard<std::mutex> acqn(lock);
      return shard.prepare(r);
    }

    void commit(uint32_t t, const Row &r) {
      std::lock_guard<std::mutex> acqn(lock);
      shard.commit(t, r);
    }

    std::vector<Cell> scan(uint32_t t) const {
      std::lock_guard<std::mutex> acqn(lock);
      return shard.scan(t);
    }

  private:
    S shard;
    mutable std::mutex lock;
};

// L is a LockSpace. S is a Shard.
template <typename L, typename S>
class ShardedTable: public Table {

  public:

    ShardedTable(size_t nlocks, size_t nshards):
      size(nshards),
      mask(nshards-1),
      shards(nshards),
      lock(nlocks)
    {}

    uint32_t time() const {
      return lock.time();
    }

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      lock.read(t, n, ks);
      for (size_t i = 0; i < n; ++i) {
        auto k = ks[i];
        vs[i] = shards[k & mask].read(t, k);
      }
    }

    uint32_t write(uint32_t t, size_t n, const Row *rs) {
      auto wt = lock.write(t, n, rs)  + 1;
      auto max = 0;
      for (size_t i = 0; i < n; ++i) {
        auto k = rs[i].k;
        auto t2 = shards[k & mask].prepare(rs[i]);
        if (max < t2)
          max = t2;
      }
      if (t < max) {
        lock.release(wt, n, rs);
        throw stale_exception(t, max);
      }
      for (size_t i = 0; i < n; ++i) {
        auto k = rs[i].k;
        shards[k & mask].commit(wt, rs[i]);
      }
      lock.release(wt, n, rs);
      return wt;
    }

    std::vector<Cell> scan() const {
      auto now = time();
      lock.scan(now);
      std::vector<Cell> cs;
      auto it = back_inserter(cs);
      for (size_t i = 0; i < size; ++i) {
        auto cs2 = shards[i].scan(now);
        std::copy(cs2.begin(), cs2.end(), it);
      }
      return cs;
    }

  private:

    const size_t size;
    const size_t mask;
    std::vector<S> shards;
    mutable L lock;
};

#endif // TABLE_HPP
