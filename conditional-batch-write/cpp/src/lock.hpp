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
#include <condition_variable>
#include <mutex>
#include <vector>

#include "table.hpp"
#include "tbb/spin_mutex.h"

/** A reader/writer lock that uses a logical clock.
  *
  * When a reader or writer acquires the lock, it supplies a timestamp, and that may raise the
  * logical time. The logical time is returned to a writer after it acquires the lock, and the
  * writer is obliged to commit writes using a timestamp strictly after that time. When the writer
  * releases the lock, it provides the write timestamp, and that may again raise the logical time.
  *
  * Only one writer at a time may hold the lock. Multiple readers may acquire the lock, some while
  * a writer holds it. Readers using a timestamp on or before the logical time will acquire the
  * lock immediately. Those using a timestamp strictly after the logical time will be blocked if a
  * writer holds the lock.
  *
  * This lock is useful with multiversion concurrency control. In a multiversion table, a reader
  * can get a value as-of some read time, even if there may be a concurrent writer, so long as that
  * writer uses a write time greater than the read time.
  *
  * @tparam M A mutex; for example, `std::mutex` or `tbb::spin_lock`.
  */
template<typename M>
class Lock {

  public:

    uint32_t time() {
      lock.lock();
      auto time = getTime(state);
      lock.unlock();
      return time;
    }

    /** A reader wants to acquire the lock; the lock will ensure no future writer commits at or
      * before that timestamp. This leaves the lock in an exclusive state; the user must call
      * `read_release` promptly.
      *
      * @param time The forecasted time; this may raise the logical time.
      */
    void read_acquire(uint32_t time) {
      lock.lock();
      while (true) {
        if (time <= getTime(state))
          return;
        if (!isHeld(state)) {
          state = makeState(time, false);
          return;
        }
        if (future < time)
          future = time;
        readers.wait(lock);
      }
    }

    /** Return the lock to a shareable state, so that other readers and a writer at a higher
      * timestamp may proceed.
      */
    void read_release() {
      lock.unlock();
    }

    /** A reader wants to acquire the lock; the lock will ensure no future writer commits at or
      * before that timestamp. This call leaves the lock in a shareable state; other readers and
      * a writer at a higher timestamp may proceed.
      *
      * @param time The forecasted time; this may raise the logical time.
      */
    void read(uint32_t time) {
      read_acquire(time);
      read_release();
    }

    /** A writer wants to acquire the lock; the lock provides the logical time, and the writer must
      * commit strictly after that time. This leaves the lock in an exclusive state; the user must
      * call `write_release` promptly.
      *
      * @param time The forecasted time; this may raise the logical time.
      * @return The logical time; the writer must commit using a timestamp strictly after this time.
      */
    uint32_t write_acquire(uint32_t time) {
      lock.lock();
      while (isHeld(state))
        writers.wait(lock);
      uint32_t now = getTime(state);
      if (now < time)
        now = time;
      future = now;
      state = makeState(now, true);
      return now;
    }

    /** Return the lock to a shareable state; other  readers at a lower timestamp may proceed.
      * The writer must eventually call `release`.
      */
    void write_release()  {
      lock.unlock();
    }

    /** A writer wants to acquire the lock; the lock provides the logical time, and the writer must
      * commit strictly after that time. This call leaves the lock in a shareable state; other
      * readers at a lower timestamp may proceed. The writer must eventually call `release`.
      *
      * @param time The forecasted time; this may raise the logical time.
      * @return The logical time; the writer must commit using a timestamp strictly after this time.
      */
    uint32_t write(uint32_t time)  {
      auto now = write_acquire(time);
      write_release();
      return now;
    }

    void release_acquire(uint32_t time) {
      lock.lock();
      if (future < time)
        future = time;
      state = makeState(future, false);
    }

    void release_release() {
      lock.unlock();
      readers.notify_all();
      writers.notify_one();
    }

    void release(uint32_t time) {
      release_acquire(time);
      release_release();
    }

  private:

    uint32_t getTime(uint32_t s) {
      return s >> 1;
    }

    bool isHeld(uint32_t s) {
      return s & 1;
    }

    uint32_t makeState (uint32_t time, bool held) {
      if (held)
        return (time << 1) | 1;
      else
        return time << 1;
    }

    M lock;
    std::condition_variable_any readers;
    std::condition_variable_any writers;
    uint32_t state = 0;
    uint32_t future = 0;
};

typedef Lock<std::mutex> StdLock;
typedef Lock<tbb::spin_mutex> TbbLock;

// L is a Lock.
template <typename L>
class LockSpace {

  public:

    LockSpace(Params &params):
      size(params.nlocks),
      mask(params.nlocks - 1),
      locks(params.nlocks)
    {}

    void read(uint32_t t, size_t n, const int *ks) {
      int is[n];
      auto end = idxs(n, ks, is);
      for (auto i = is; i < end; ++i)
        locks[*i].read(t);
    }

    void read(uint32_t t, std::vector<int> ks) {
      read(t, ks.size(), ks.data());
    }

    uint32_t write(uint32_t t, size_t n, const Row *rs) {
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
      int is[n];
      auto end = idxs(n, rs, is);
      for (auto i = is; i < end; ++i)
        locks[*i].release(t);
    }

    void release(uint32_t t, std::vector<Row> rs) {
      release(t, rs.size(), rs.data());
    }

    uint32_t scan() {
      uint32_t t = 0;
      for (size_t i = 0; i < size; ++i) {
        auto _t = locks[i].time();
        if (t < _t)
          t = _t;
      }
      for (size_t i = 0; i < size; ++i)
        locks[i].read(t);
      return t;
    }

  private:

    const size_t size;
    const size_t mask;
    std::vector<L> locks;

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
