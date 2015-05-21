package experiments

import java.lang.{Integer => JInt}
import java.util.concurrent.{Callable, Executors, ExecutionException}
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

/** Use `SingleThreadedExecutor` to make the shard thread safe. */
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

  def apply (nlocks: Int, nshards: Int) (newShard: => Shard): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (newShard)
  }}

trait NewSynchronizedShardedTable extends NewTable {

  def parallel = true

  def newTable =
    ShardedTable (nlocks, nshards) (new SynchronizedShard (newRecommendedShard))
}

trait NewReadWriteShardedTable extends NewTable {

  def parallel = true

  def newTable =
    ShardedTable (nlocks, nshards) (new ReadWriteShard (newRecommendedShard))
}

trait NewSingleThreadShardedTable extends NewTable {

  def parallel = true

  def newTable =
    ShardedTable (nlocks, nshards) (new SingleThreadShard (newRecommendedShard, newRecommendedScheduler))
}
