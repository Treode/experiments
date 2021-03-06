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

#include <cassert>
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

    virtual void read(uint32_t t, size_t n, const int *ks, Value *vs) const = 0;

    virtual bool write(uint32_t ct, size_t n, const Row *rs, uint32_t &wt) = 0;

    virtual void scan(std::vector<Cell> &cs) const =  0;

    std::vector<Value> read(uint32_t t, const std::vector<int> &ks) const {
      auto vs = std::vector<Value>(ks.size());
      read(t, ks.size(), ks.data(), vs.data());
      return vs;
    }

    bool write(uint32_t ct, const std::vector<Row> &rs, uint32_t &wt) {
      return write(ct, rs.size(), rs.data(), wt);
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

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      raise(t);
      for (size_t i = 0; i < n; ++i)
        shard.read(t, ks[i], vs[i]);
    }

    bool write(uint32_t ct, size_t n, const Row *rs, uint32_t &wt) {
      raise(ct);
      auto vt = prepare(n, rs);
      if (ct < vt) {
        wt = vt;
        return false;
      }
      wt = ++clock;
      commit(wt, n, rs);
      return true;
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

    uint32_t prepare(size_t n, const Row *rs) const {
      uint32_t vt = 0;
      for (size_t i = 0; i < n; ++i) {
        auto _vt = shard.prepare(rs[i].k);
        if (vt < _vt)
          vt = _vt;
      }
      return vt;
    }

    void commit(uint32_t t, size_t n, const Row *rs) {
      for (size_t i = 0; i < n; ++i)
        shard.commit(t, rs[i].k, rs[i].v);
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

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      lock.read(t, n, ks);
      for (size_t i = 0; i < n; ++i) {
        auto k = ks[i];
        shards[k & mask].read(t, k, vs[i]);
      }
    }

    bool write(uint32_t ct, size_t n, const Row *rs, uint32_t &wt) {
      wt = lock.write(ct, n, rs) + 1;
      uint32_t vt = 0;
      for (size_t i = 0; i < n; ++i) {
        auto k = rs[i].k;
        auto _vt = shards[k & mask].prepare(rs[i].k);
        if (vt < _vt)
          vt = _vt;
      }
      if (ct < vt) {
        lock.release(wt, n, rs);
        wt = vt;
        return false;
      }
      for (size_t i = 0; i < n; ++i) {
        auto &r = rs[i];
        shards[r.k & mask].commit(wt, r.k, r.v);
      }
      lock.release(wt, n, rs);
      return true;
    }

    void scan(std::vector<Cell> &cs) const {
      auto t = lock.scan();
      for (size_t i = 0; i < size; ++i)
        shards[i].scan(t, cs);
    }

  private:

    const size_t size;
    const size_t mask;
    std::vector<S> shards;
    mutable L lock;
};

class LockingShard {

  public:

    virtual ~LockingShard() = default;

    virtual uint32_t time() const = 0;

    virtual void read(uint32_t t, int k, Value &v) const = 0;

    virtual void prepare(uint32_t ct, int k, uint32_t &vt, uint32_t &lt) const = 0;

    virtual void prepare(uint32_t ct, int k1, int k2, uint32_t &vt, uint32_t &lt) const = 0;

    virtual void commit(uint32_t t, int k, int v) = 0;

    virtual void commit(uint32_t t, int k1, int v1, int k2, int v2) = 0;

    virtual void abort() = 0;

    virtual void scan(uint32_t t, std::vector<Cell> &cs) const = 0;
};

// S is a Shard.
template <typename S>
class LockingShardTable: public Table {

  public:

    LockingShardTable(Params &params):
      size(params.nshards),
      mask(params.nshards-1),
      shards(params.nshards)
    {}

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const {
      for (size_t i = 0; i < n; ++i) {
        auto k = ks[i];
        shards[k & mask].read(t, k, vs[i]);
      }
    }

    bool write(uint32_t ct, size_t n, const Row *rs, uint32_t &wt) {
      bool r;
      if (n == 1)
        r = _write(ct, rs[0], wt);
      else if (n == 2)
        r = _write(ct, rs[0], rs[1], wt);
      else
        assert(false);
      return r;
    }

    void scan(std::vector<Cell> &cs) const {
      uint32_t t = 0;
      for (size_t i = 0; i < size; ++i) {
        auto _t = shards[i].time();
        if (t < _t)
          t = _t;
      }
      for (size_t i = 0; i < size; ++i)
        shards[i].scan(t, cs);
    }

  private:

    const size_t size;
    const size_t mask;
    std::vector<S> shards;

    bool _write(uint32_t t, const Row &r, uint32_t &wt) {
      int i = r.k & mask;
      uint32_t vt = 0, lt = 0;
      shards[i].prepare(t, r.k, vt, lt);
      if (t < vt) {
        shards[i].abort();
        wt = vt;
        return false;
      }
      wt = lt + 1;
      shards[i].commit(wt, r.k, r.v);
      return true;
    }

    bool _write(uint32_t t, const Row &r1, const Row &r2, uint32_t &wt) {
      int i1 = r1.k & mask;
      int i2 = r2.k & mask;
      uint32_t vt = 0, lt = 0;
      if (i1 < i2) {
        shards[i1].prepare(t, r1.k, vt, lt);
        shards[i2].prepare(t, r2.k, vt, lt);
      } else if (i2 < i1) {
        shards[i2].prepare(t, r2.k, vt, lt);
        shards[i1].prepare(t, r1.k, vt, lt);
      } else {
        shards[i1].prepare(t, r1.k, r2.k, vt, lt);
      }
      if (t < vt) {
        if (i1 == i2) {
          shards[i1].abort();
        } else {
          shards[i1].abort();
          shards[i2].abort();
        }
        wt = vt;
        return false;
      }
      wt = lt + 1;
      if (i1 == i2) {
        shards[i1].commit(wt, r1.k, r1.v, r2.k, r2.v);
      } else {
        shards[i1].commit(wt, r1.k, r1.v);
        shards[i2].commit(wt, r2.k, r2.v);
      }
      return true;
    }
};

unsigned broker(Table &table, const Params &params);

#endif // TABLE_HPP
