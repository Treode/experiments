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

package experiments

import java.lang.{Integer => JInt}
import java.util.concurrent.{CountDownLatch, Future}
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantReadWriteLock

/** A shard of the hash table. Whereas Table provides conditional batch write directly, this
  * trait provides prepare and commit that ShardedTable uses to coordinate a conditinal batch
  * write across many shards.
  */
trait Shard {

  /** Read key `k` as of time `t`. */
  def read (t: Int, k: Int): Value

  /** Find the latest timestamp for key `r.k`. */
  def prepare (r: Row): Int

  /** Write row `r` as of time `t`. */
  def commit (t: Int, r: Row)

  /** Scan the history before and including time `t`. Not part of the performance timings. */
  def scan (t: Int): Seq [Cell]

  def close()
}

/** Use `synchronized` to make the shard thread safe. */
class SynchronizedShard (shard: Shard) extends Shard {

  def read (t: Int, k: Int): Value =
    synchronized (shard.read (t, k))

  def prepare (r: Row): Int =
    synchronized (shard.prepare (r))

  def commit (t: Int, r: Row): Unit =
    synchronized (shard.commit (t, r))

  def scan (t: Int): Seq [Cell] =
    synchronized (shard.scan (t: Int))

  def close(): Unit = ()
}

/** Use a reader-writer lock to make the shard thread safe. */
class ReadWriteShard (shard: Shard) extends Shard {

  private val lock = new ReentrantReadWriteLock

  def read (t: Int, k: Int): Value = {
    lock.readLock.lock()
    try (shard.read (t, k))
    finally (lock.readLock.unlock())
  }

  def prepare (r: Row): Int = {
    lock.readLock.lock()
    try (shard.prepare (r))
    finally (lock.readLock.unlock())
  }

  def commit (t: Int, r: Row): Unit = {
    lock.writeLock.lock()
    try (shard.commit (t, r))
    finally (lock.writeLock.unlock())
  }

  def scan (t: Int): Seq [Cell] = {
    lock.readLock.lock()
    try (shard.scan (t: Int))
    finally (lock.readLock.unlock())
  }

  def close(): Unit = ()
}

/** Use `SingleThreadedScheduler` to make the shard thread safe. */
class SingleThreadShard (shard: Shard, scheduler: SingleThreadScheduler) extends Shard {

  def read (t: Int, k: Int): Value =
    scheduler.submit (shard.read (t, k)) .safeGet

  def prepare (r: Row): Int =
    scheduler.submit (shard.prepare (r)) .safeGet

  def commit (t: Int, r: Row): Unit =
    scheduler.execute (shard.commit (t, r))

  def scan (t: Int): Seq [Cell] =
    scheduler.submit (shard.scan (t: Int)) .safeGet

  def close(): Unit =
    scheduler.shutdown()
}

/** Shard the key space over many tables. */
class ShardedTable private (lock: LockSpace, mask: Int) (newShard: => Shard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  def time = lock.time

  private def read (t: Int, k: Int): Value =
    shards (k & mask) .read (t, k)

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    ks map (read (t, _))
  }

  private def prepare (r: Row): Int =
    shards (r.k & mask) .prepare (r)

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = rs.map (prepare (_)) .max
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (t: Int, r: Row): Unit =
    shards (r.k & mask) .commit (t, r)

  private def commit (t: Int, rs: Seq [Row]): Unit =
    rs foreach (commit (t, _))

  def write (t: Int, rs: Row*): Int = {
    val wt = lock.write (lock.time, rs) + 1
    try {
      prepare (t, rs)
      commit (wt, rs)
      wt
    } finally {
      lock.release (wt, rs)
    }}

  def scan(): Seq [Cell] = {
    val now = lock.time
    lock.scan (now)
    shards.map (_.scan (now)) .flatten.toSeq
  }

  def close(): Unit =
    shards foreach (_.close())
}

object ShardedTable {

  def apply (lock: LockSpace) (newShard: => Shard) (implicit params: Params): ShardedTable = {
    import params.nshards;
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (lock, nshards - 1) (newShard)
  }}

trait NewSynchronizedShardedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    ShardedTable (AqsLock.newSpace) (new SynchronizedShard (newRecommendedShard))
}

trait NewConditionLockTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    ShardedTable (ConditionLock.newSpace) (new SynchronizedShard (newRecommendedShard))
}

trait NewReadWriteShardedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    ShardedTable (AqsLock.newSpace) (new ReadWriteShard (newRecommendedShard))
}

trait NewSingleThreadShardedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    ShardedTable (AqsLock.newSpace) (new SingleThreadShard (newRecommendedShard, newRecommendedScheduler))
}

/** A shard that returns futures. */
class FutureShard (shard: Shard, scheduler: SingleThreadScheduler) {

  def read (t: Int, k: Int): Future [Value] =
    scheduler.submit (shard.read (t, k))

  def prepare (r: Row): Future [Int] =
    scheduler.submit (shard.prepare (r))

  def commit (t: Int, r: Row): Unit =
    scheduler.execute (shard.commit (t, r))

  def scan (t: Int): Future [Seq [Cell]] =
    scheduler.submit (shard.scan (t: Int))

  def close(): Unit =
    scheduler.shutdown()
}

/** Shard the key space over many tables. */
class FutureShardedTable private (lock: LockSpace, mask: Int) (newShard: => FutureShard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  def time = lock.time

  private def read (t: Int, k: Int): Future [Value] =
    shards (k & mask) .read (t, k)

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    ks.map (read (t, _)) .map (_.get)
  }

  private def prepare (r: Row): Future [Int] =
    shards (r.k & mask) .prepare (r)

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = rs.map (prepare (_)) .map (_.get) .max
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (t: Int, r: Row): Unit =
    shards (r.k & mask) .commit (t, r)

  private def commit (t: Int, rs: Seq [Row]): Unit =
    rs foreach (commit (t, _))

  def write (t: Int, rs: Row*): Int = {
    val wt = lock.write (lock.time, rs) + 1
    try {
      prepare (t, rs)
      commit (wt, rs)
      wt
    } finally {
      lock.release (wt, rs)
    }}

  def scan(): Seq [Cell] = {
    val now = lock.time
    lock.scan (now)
    shards.map (_.scan (now)) .map (_.get) .flatten.toSeq
  }

  def close(): Unit =
    shards foreach (_.close())
}

object FutureShardedTable {

  def apply (lock: LockSpace) (newShard: => FutureShard) (implicit params: Params): FutureShardedTable = {
    import params.nshards
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new FutureShardedTable (lock, nshards - 1) (newShard)
  }}

trait NewFutureShardedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    FutureShardedTable (AqsLock.newSpace) (new FutureShard (newRecommendedShard, newRecommendedScheduler))
}

/** Collect `n` results. */
class Collector [A: Manifest] (n: Int) {

  private val vs = new Array [A] (n)
  private val count = new CountDownLatch (n)

  /** Set the result for index `i`. */
  def set (i: Int, v: A) {
    vs (i) = v
    count.countDown() // Memory barrier for array set above.
  }

  def result: Seq [A] = {
    count.await() // Memory barrier for array read below.
    vs.toSeq
  }}

/** A shard that sends its results to a collector. */
class CollectorShard (shard: Shard, scheduler: SingleThreadScheduler) {

  def read (t: Int, k: Int, i: Int, c: Collector [Value]): Unit =
    scheduler.execute (c.set (i, shard.read (t, k)))

  def prepare (r: Row, i: Int, c: Collector [Int]): Unit =
    scheduler.execute (c.set (i, shard.prepare (r)))

  def commit (t: Int, r: Row): Unit =
    scheduler.execute (shard.commit (t, r))

  def scan (t: Int, i: Int, c: Collector [Seq [Cell]]): Unit =
    scheduler.submit (c.set (i, shard.scan (t: Int)))

  def close(): Unit =
    scheduler.shutdown()
}

/** Shard the key space over many tables. */
class CollectorShardedTable private (lock: LockSpace, mask: Int) (newShard: => CollectorShard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  def time = lock.time

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    val c = new Collector [Value] (ks.size)
    for ((k, i) <- ks.zipWithIndex)
      shards (k & mask) .read (t, k, i, c)
    c.result
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val c = new Collector [Int] (rs.size)
    for ((r, i) <- rs.zipWithIndex)
      shards (r.k & mask) .prepare (r, i, c)
    val max = c.result.max
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (t: Int, rs: Seq [Row]): Unit =
    for (r <- rs)
      shards (r.k & mask) .commit (t, r)

  def write (t: Int, rs: Row*): Int = {
    val wt = lock.write (lock.time, rs) + 1
    try {
      prepare (t, rs)
      commit (wt, rs)
      wt
    } finally {
      lock.release (wt, rs)
    }}

  def scan(): Seq [Cell] = {
    val now = lock.time
    lock.scan (now)
    val c = new Collector [Seq [Cell]] (mask + 1)
    for ((s, i) <- shards.zipWithIndex)
      s.scan (now, i, c)
    c.result.flatten.toSeq
  }

  def close(): Unit =
    shards foreach (_.close())
}

object CollectorShardedTable {

  def apply (lock: LockSpace) (newShard: => CollectorShard) (implicit params: Params): CollectorShardedTable = {
    import params.nshards
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new CollectorShardedTable (lock, nshards - 1) (newShard)
  }}

trait NewCollectorShardedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    CollectorShardedTable (AqsLock.newSpace) (new CollectorShard (newRecommendedShard, newRecommendedScheduler))
}
