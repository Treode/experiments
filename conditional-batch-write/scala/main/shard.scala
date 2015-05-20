package experiments

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
class SingleThreadShard (shard: Shard) extends Shard {

  private val executor = Executors.newSingleThreadExecutor

  private def execute (f: => Any): Unit =
    executor.execute (new Runnable {
      def run() = f
    })

  private def submit [A] (f: => A): A =
    try {
      executor.submit (new Callable [A] {
        def call(): A = f
      }) .get
    } catch {
      case t: ExecutionException =>
        throw t.getCause
    }

  def read (t: Int, k: Int): Value =
    submit (shard.read (t, k))

  def prepare (r: Row): Int =
    submit (shard.prepare (r))

  def commit (t: Int, r: Row): Unit =
    execute (shard.commit (t, r))

  def scan (t: Int): Seq [Cell] =
    submit (shard.scan (t: Int))

  def close(): Unit =
    executor.shutdown()
}