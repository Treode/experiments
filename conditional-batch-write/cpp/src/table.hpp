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

#include "tbb/spin_mutex.h"

struct Params {

  std::string platform;
  size_t nlocks;
  size_t nshards;
  size_t naccounts;
  size_t nbrokers;
  size_t ntransfers;
  size_t nreads;

  Params(
    const std::string &_platform,
    size_t _nlocks,
    size_t _nshards,
    size_t _naccounts,
    size_t _nbrokers,
    size_t _ntransfers,
    size_t _nreads
  ):
    platform(_platform),
    nlocks(_nlocks),
    nshards(_nshards),
    naccounts(_naccounts),
    nbrokers(_nbrokers),
    ntransfers(_ntransfers),
    nreads(_nreads)
  {}
};

std::ostream &operator<<(std::ostream &os, const Params p);

struct Value {

  int v;
  uint32_t t;

  constexpr Value(): v(0), t(0) {}

  constexpr Value(int _v, uint32_t _t): v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Value &v);

bool operator==(const Value &x, const Value &y);

inline bool operator!=(const Value &x, const Value &y) {
  return !(x == y);
}

struct Row {

  int k, v;

  constexpr Row(): k(0), v(0) {}

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

  constexpr Cell(): k(0), v(0), t(0) {}

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

class Shard {

  public:

    virtual ~Shard() = default;

    virtual void read(uint32_t t, int k, Value &v) const = 0;

    virtual uint32_t prepare(int k) const = 0;

    virtual void commit(uint32_t t, int k, int v) = 0;

    virtual void scan(uint32_t t, std::vector<Cell> &cs) const = 0;
};

// S is a shard
template <typename S>
class StdMutexShard: public Shard {

  public:

    void read(uint32_t t, int k, Value &v) const {
      std::lock_guard<std::mutex> acqn(lock);
      shard.read(t, k, v);
    }

    uint32_t prepare(int k) const {
      std::lock_guard<std::mutex> acqn(lock);
      return shard.prepare(k);
    }

    void commit(uint32_t t, int k, int v) {
      std::lock_guard<std::mutex> acqn(lock);
      shard.commit(t, k, v);
    }

    void scan(uint32_t t, std::vector<Cell> &cs) const {
      std::lock_guard<std::mutex> acqn(lock);
      return shard.scan(t, cs);
    }

  private:
    S shard;
    mutable std::mutex lock;
};

// S is a shard
template <typename S>
class TbbMutexShard: public Shard {

  public:

    void read(uint32_t t, int k, Value &v) const {
      tbb::spin_mutex::scoped_lock acqn(lock);
      shard.read(t, k, v);
    }

    uint32_t prepare(int k) const {
      tbb::spin_mutex::scoped_lock acqn(lock);
      return shard.prepare(k);
    }

    void commit(uint32_t t, int k, int v) {
      tbb::spin_mutex::scoped_lock acqn(lock);
      shard.commit(t, k, v);
    }

    void scan(uint32_t t, std::vector<Cell> &cs) const {
      tbb::spin_mutex::scoped_lock acqn(lock);
      return shard.scan(t, cs);
    }

  private:
    S shard;
    mutable tbb::spin_mutex lock;
};

class Table {

  public:

    virtual ~Table() = default;

    virtual uint32_t time() const = 0;

    virtual void read(uint32_t t, size_t n, const int *ks, Value *vs) const = 0;

    virtual uint32_t write(uint32_t t, size_t n, const Row *rs) = 0;

    virtual void scan(std::vector<Cell> &cs) const =  0;

    std::vector<Value> read(uint32_t t, const std::vector<int> &ks) const {
      auto vs = std::vector<Value>(ks.size());
      read(t, ks.size(), ks.data(), vs.data());
      return vs;
    }

    uint32_t write(uint32_t t, const std::vector<Row> &rs) {
      return write(t, rs.size(), rs.data());
    }

    std::vector<Cell> scan() const {
      std::vector<Cell> cs;
      scan(cs);
      return cs;
    }
};

std::ostream &operator<<(std::ostream &os, const Table &table);

// S is a shard
template<typename S>
class TableFromShard: public Table {

  public:

    uint32_t time() const {
      return clock;
    }

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      raise(t);
      for (size_t i = 0; i < n; ++i)
        shard.read(t, ks[i], vs[i]);
    }

    uint32_t write(uint32_t t, size_t n, const Row *rs) {
      raise(t);
      prepare(t, n, rs);
      return commit(n, rs);
    }

    void scan(std::vector<Cell> &cs) const {
      shard.scan(clock, cs);
    }

  private:

    mutable uint32_t clock = 0;

    S shard;

    void raise(uint32_t t) const {
      if (clock < t)
        clock = t;
    }

    void prepare(uint32_t t, size_t n, const Row *rs) const {
      uint32_t max = 0;
      for (size_t i = 0; i < n; ++i) {
        auto t2 = shard.prepare(rs[i].k);
        if (max < t2)
          max = t2;
      }
      if (max > t)
        throw stale_exception(t, max);
    }

    uint32_t commit(size_t n, const Row *rs) {
      auto t = ++clock;
      for (size_t i = 0; i < n; ++i)
        shard.commit(clock, rs[i].k, rs[i].v);
      return clock;
    }
};

// L is a LockSpace. S is a Shard.
template <typename L, typename S>
class ShardedTable: public Table {

  public:

    ShardedTable(Params &params):
      size(params.nshards),
      mask(params.nshards-1),
      shards(params.nshards),
      lock(params)
    {}

    uint32_t time() const {
      return lock.time();
    }

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      lock.read(t, n, ks);
      for (size_t i = 0; i < n; ++i) {
        auto k = ks[i];
        shards[k & mask].read(t, k, vs[i]);
      }
    }

    uint32_t write(uint32_t t, size_t n, const Row *rs) {
      auto wt = lock.write(t, n, rs)  + 1;
      auto max = 0;
      for (size_t i = 0; i < n; ++i) {
        auto k = rs[i].k;
        auto t2 = shards[k & mask].prepare(rs[i].k);
        if (max < t2)
          max = t2;
      }
      if (t < max) {
        lock.release(wt, n, rs);
        throw stale_exception(t, max);
      }
      for (size_t i = 0; i < n; ++i) {
        auto &r = rs[i];
        shards[r.k & mask].commit(wt, r.k, r.v);
      }
      lock.release(wt, n, rs);
      return wt;
    }

    void scan(std::vector<Cell> &cs) const {
      auto t = time();
      lock.scan(t);
      for (size_t i = 0; i < size; ++i)
        shards[i].scan(t, cs);
    }

  private:

    const size_t size;
    const size_t mask;
    std::vector<S> shards;
    mutable L lock;
};

unsigned broker(Table &table, const Params &params);

#endif // TABLE_HPP
