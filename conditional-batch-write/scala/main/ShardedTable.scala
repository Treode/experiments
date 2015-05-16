package experiments

import java.lang.{Integer => JInt}
import java.util.{HashMap, TreeMap}
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConversions._

/** Shard the key space over many tables. */
class ShardedTable private (lock: LockSpace, mask: Int) (newShard: => ShardedTable.Shard)
extends Table {

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
    val max = Int.MaxValue - (rs.map (prepare (_)) .min)
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

  /** A shard of the hash table. Whereas Table provides conditional batch write directly, this
    * trait provides prepare and commit that ShardedTable uses to coordinate a conditinal batch
    * write across many shards.
    */
  trait Shard {

    def read (t: Int, k: Int): Value
    def prepare (r: Row): Int
    def commit (t: Int, r: Row)
    def scan (t: Int): Seq [Cell]
    def close()
  }

  /** A shard similar to JavaHashMapOfTreeMap. This is not thread safe. */
  class JavaHashMapOfTreeMapShard extends Shard {

    /** A constant. */
    private val empty = new TreeMap [Int, Int]

    private val table = new HashMap [Int, TreeMap [Int, Int]]

    def read (t: Int, k: Int): Value = {
      val x = Int.MaxValue - t
      val vs = table.get (k)
      if (vs == null)
        return Value.empty
      val i = vs.tailMap (x)
      if (i.isEmpty)
        return Value.empty
      val (x2, v) = i.head
      return Value (v, Int.MaxValue - x2)
    }

    def prepare (r: Row): Int = {
      val vs = table.get (r.k)
      if (vs == null)
        return Int.MaxValue
      return vs.firstKey
    }

    def commit (t: Int, r: Row) {
      val x = Int.MaxValue - t
      var vs = table.get (r.k)
      if (vs == null) {
        vs = new java.util.TreeMap [Int, Int]
        table.put (r.k, vs)
      }
      vs.put (x, r.v)
    }

    def scan (t: Int): Seq [Cell] =
      for {
        (k, vs) <- table.toSeq
        (x, v) <- vs
        t2 = Int.MaxValue - x
        if t2 < t
      } yield Cell (k, v, t2)

    def close() = ()
  }

  /** Use `synchronized` to make the shard thread safe. */
  class SynchronizedShard extends Shard {

    private val shard = new JavaHashMapOfTreeMapShard

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
  class ReadWriteShard extends Shard {

    private val shard = new JavaHashMapOfTreeMapShard
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
  class SingleThreadShard extends Shard {

    private val shard = new JavaHashMapOfTreeMapShard
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

  def synchronizedShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (new SynchronizedShard)
  }

  def readWriteShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (new ReadWriteShard)
  }

  def singleThreadShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (new SingleThreadShard)
  }
}

trait NewSynchronizedShardedTable extends NewTable {

  def parallel = true

  def newTable = ShardedTable.synchronizedShards (8, 8)
}

trait NewReadWriteShardedTable extends NewTable {

  def parallel = true

  def newTable = ShardedTable.readWriteShards (8, 8)
}

trait NewSingleThreadShardedTable extends NewTable {

  def parallel = true

  def newTable = ShardedTable.synchronizedShards (8, 8)
}
