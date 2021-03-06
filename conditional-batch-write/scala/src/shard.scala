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

  /** Find the latest timestamp for key `k`. */
  def prepare (k: Int): Int

  /** Set key `k` to value `v` at time `t`. */
  def commit (t: Int, k: Int, v: Int)

  /** Scan the history before and including time `t`. Not part of the performance timings. */
  def scan (t: Int): Seq [Cell]

  def close()
}

/** Use `synchronized` to make the shard thread safe. */
class SynchronizedShard (shard: Shard) extends Shard {

  def read (t: Int, k: Int): Value =
    synchronized (shard.read (t, k))

  def prepare (k: Int): Int =
    synchronized (shard.prepare (k))

  def commit (t: Int, k: Int, v: Int): Unit =
    synchronized (shard.commit (t, k, v))

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

  def prepare (k: Int): Int = {
    lock.readLock.lock()
    try (shard.prepare (k))
    finally (lock.readLock.unlock())
  }

  def commit (t: Int, k: Int, v: Int): Unit = {
    lock.writeLock.lock()
    try (shard.commit (t, k, v))
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

  def prepare (k: Int): Int =
    scheduler.submit (shard.prepare (k)) .safeGet

  def commit (t: Int, k: Int, v: Int): Unit =
    scheduler.execute (shard.commit (t, k, v))

  def scan (t: Int): Seq [Cell] =
    scheduler.submit (shard.scan (t: Int)) .safeGet

  def close(): Unit =
    scheduler.shutdown()
}

/** Convert a shard to a thread-unsafe table. */
class TableFromShard (shard: Shard) extends Table {

  private var clock = 0

  private def raise (t: Int): Unit =
    if (clock < t)
      clock = t

  def time = clock

  def read (t: Int, ks: Int*): Seq [Value] = {
    raise (t)
    ks map (shard.read (t, _))
  }

  private def prepare (r: Row): Int =
    shard.prepare (r.k)

  private def prepare (rs: Seq [Row]): Int =
    rs.map (prepare (_)) .max

  private def commit (t: Int, r: Row): Unit =
    shard.commit (t, r.k, r.v)

  private def commit (rs: Seq [Row]): Int = {
    clock += 1
    rs foreach (commit (clock, _))
    clock
  }

  def write (ct: Int, rs: Row*): Either [Int, Int] = {
    raise (ct)
    val vt = prepare (rs)
    if (ct < vt)
      Right (vt)
    else
      Left (commit (rs))
  }

  def scan(): Seq [Cell] =
    shard.scan (clock)

  def close() = shard.close()
}

/** Shard the key space over many tables. */
class ShardedTable private (lock: LockSpace, mask: Int) (newShard: => Shard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  private def read (t: Int, k: Int): Value =
    shards (k & mask) .read (t, k)

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    ks map (read (t, _))
  }

  private def prepare (r: Row): Int =
    shards (r.k & mask) .prepare (r.k)

  private def prepare (rs: Seq [Row]): Int =
    rs.map (prepare (_)) .max

  private def commit (t: Int, r: Row): Unit =
    shards (r.k & mask) .commit (t, r.k, r.v)

  private def commit (t: Int, rs: Seq [Row]): Unit =
    rs foreach (commit (t, _))

  def write (ct: Int, rs: Row*): Either [Int, Int] = {
    val wt = lock.write (ct, rs) + 1
    val vt = prepare (rs)
    if (ct < vt) {
      lock.release (wt, rs)
      return Right (vt)
    }
    commit (wt, rs)
    lock.release (wt, rs)
    Left (wt)
  }

  def scan(): Seq [Cell] = {
    val t = lock.scan()
    shards.map (_.scan (t)) .flatten.toSeq
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

  def prepare (k: Int): Future [Int] =
    scheduler.submit (shard.prepare (k))

  def commit (t: Int, k: Int, v: Int): Unit =
    scheduler.execute (shard.commit (t, k, v))

  def scan (t: Int): Future [Seq [Cell]] =
    scheduler.submit (shard.scan (t: Int))

  def close(): Unit =
    scheduler.shutdown()
}

/** Shard the key space over many tables. */
class FutureShardedTable private (lock: LockSpace, mask: Int) (newShard: => FutureShard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  private def read (t: Int, k: Int): Future [Value] =
    shards (k & mask) .read (t, k)

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    ks.map (read (t, _)) .map (_.get)
  }

  private def prepare (r: Row): Future [Int] =
    shards (r.k & mask) .prepare (r.k)

  private def prepare (rs: Seq [Row]): Int =
    rs.map (prepare (_)) .map (_.get) .max

  private def commit (t: Int, r: Row): Unit =
    shards (r.k & mask) .commit (t, r.k, r.v)

  private def commit (t: Int, rs: Seq [Row]): Unit =
    rs foreach (commit (t, _))

  def write (ct: Int, rs: Row*): Either [Int, Int] = {
    val wt = lock.write (ct, rs) + 1
    val vt = prepare (rs)
    if (ct < vt) {
      lock.release (wt, rs)
      return Right (vt)
    }
    commit (wt, rs)
    lock.release (wt, rs)
    Left (wt)
  }

  def scan(): Seq [Cell] = {
    val t = lock.scan()
    shards.map (_.scan (t)) .map (_.get) .flatten.toSeq
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

  def prepare (k: Int, i: Int, c: Collector [Int]): Unit =
    scheduler.execute (c.set (i, shard.prepare (k)))

  def commit (t: Int, k: Int, v: Int): Unit =
    scheduler.execute (shard.commit (t, k, v))

  def scan (t: Int, i: Int, c: Collector [Seq [Cell]]): Unit =
    scheduler.submit (c.set (i, shard.scan (t: Int)))

  def close(): Unit =
    scheduler.shutdown()
}

/** Shard the key space over many tables. */
class CollectorShardedTable private (lock: LockSpace, mask: Int) (newShard: => CollectorShard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    val c = new Collector [Value] (ks.size)
    for ((k, i) <- ks.zipWithIndex)
      shards (k & mask) .read (t, k, i, c)
    c.result
  }

  private def prepare (rs: Seq [Row]): Int = {
    val c = new Collector [Int] (rs.size)
    for ((r, i) <- rs.zipWithIndex)
      shards (r.k & mask) .prepare (r.k, i, c)
    c.result.max
  }

  private def commit (t: Int, rs: Seq [Row]): Unit =
    for (r <- rs)
      shards (r.k & mask) .commit (t, r.k, r.v)

  def write (ct: Int, rs: Row*): Either [Int, Int] = {
    val wt = lock.write (ct, rs) + 1
    val vt = prepare (rs)
    if (ct < vt) {
      lock.release (wt, rs)
      return Right (vt)
    }
    commit (wt, rs)
    lock.release (wt, rs)
    Left (wt)
  }

  def scan(): Seq [Cell] = {
    val t = lock.scan()
    val c = new Collector [Seq [Cell]] (mask + 1)
    for ((s, i) <- shards.zipWithIndex)
      s.scan (t, i, c)
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

/** A self-locking shard of the hash table. Whereas ShardedTable uses manages the logical locks
  * directly, and then requires the shards to manage concurrent access to their data structures,
  * a LockingShard incorporates the logical lock into its operations.
  */
trait LockingShard {

  def time: Int

  /** Read key `k` as of time `t`. */
  def read (t: Int, k: Int): Value

  /** Find the latest timestamp for key `k`, and the logical time. The writer must eventually call
    * `commit` or `abort`.
    */
  def prepare (t: Int, k: Int): (Int, Int)

  /** Find the latest timestamp for the keys `k1` and `k2`, and the logical time. The writer must
    * eventually call `commit` or `abort`.*/
  def prepare (t: Int, k1: Int, k2: Int): (Int, Int)

  /** Set key `k` to value `v` at time `t`. */
  def commit (t: Int, k: Int, v: Int)

  /** Set key `k1` to value `v1` and `k2` to value `v2` at time `t`. */
  def commit (t: Int, k1: Int, v1: Int, k2: Int, v2: Int)

  /** Abort an earlier prepare. */
  def abort()

  /** Scan the history before and including time `t`. Not part of the performance timings. */
  def scan (t: Int): Seq [Cell]
}

class LockingShardTable private (mask: Int) (newShard: => LockingShard) extends Table {

  private val shards = Array.fill (mask + 1) (newShard)

  private def read (t: Int, k: Int): Value =
    shards (k & mask) .read (t, k)

  def read (t: Int, ks: Int*): Seq [Value] =
    ks map (read (t, _))

  private def write (ct: Int, r: Row): Either [Int, Int] = {
    val i = r.k & mask
    val (vt, lt) = shards (i) .prepare (ct, r.k)
    if (ct < vt) {
      shards (i) .abort()
      return Right (vt)
    }
    val wt = lt + 1
    shards (i) .commit (wt, r.k, r.v)
    Left (wt)
  }

  private def write (ct: Int, r1: Row, r2: Row): Either [Int, Int] = {

    val i1 = r1.k & mask
    val i2 = r2.k & mask
    var vt = 0
    var lt = 0

    def prepare1 (i: Int, ct: Int, k: Int) {
      val (_vt1, _lt1) = shards (i) .prepare (ct, k)
      if (vt < _vt1)
        vt = _vt1
      if (lt < _lt1)
        lt = _lt1
    }

    def prepare2 (i: Int, ct: Int, k1: Int, k2: Int) {
      val (_vt1, _lt1) = shards (i) .prepare (ct, k1, k2)
      if (vt < _vt1)
        vt = _vt1
      if (lt < _lt1)
        lt = _lt1
    }

    if (i1 < i2) {
      prepare1 (i1, ct, r1.k)
      prepare1 (i2, ct, r2.k)
    } else if (i2 < i1) {
      prepare1 (i2, ct, r2.k)
      prepare1 (i1, ct, r1.k)
    } else {
      prepare2 (i1, ct, r1.k, r2.k)
    }
    if (ct < vt) {
      if (i1 == i2) {
        shards (i1) .abort()
      } else {
        shards (i1) .abort()
        shards (i2) .abort()
      }
      return Right (vt)
    }
    val wt = lt + 1;
    if (i1 == i2) {
      shards (i1) .commit (wt, r1.k, r1.v, r2.k, r2.v)
    } else {
      shards (i1) .commit (wt, r1.k, r1.v)
      shards (i2) .commit (wt, r2.k, r2.v)
    }
    Left (wt)
  }

  def write (ct: Int, rs: Row*): Either [Int, Int] = {
    if (rs.size == 1)
      write (ct, rs(0));
    else if (rs.size == 2)
      write (ct, rs(0), rs(1));
    else
      throw new Exception;
  }

  def scan(): Seq [Cell] = {
    var t = 0
    for (s <- shards) {
      val _t = s.time
      if (t < _t)
        t = _t
    }
    val b = Seq.newBuilder [Cell]
    for (s <- shards)
      b ++= s.scan (t)
    b.result
  }

  def close() = ()
}

object LockingShardTable {

  def apply (newShard: => LockingShard) (implicit params: Params): LockingShardTable = {
    import params.nshards
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new LockingShardTable (nshards - 1) (newShard)
  }}
