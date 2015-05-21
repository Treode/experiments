package experiments

import java.lang.{Integer => JInt}
import java.util.{HashMap, TreeMap}
import java.util.concurrent._
import scala.collection.JavaConversions._

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

  def synchronizedShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (new SynchronizedShard (new JavaHashMapOfTreeMap))
  }

  def readWriteShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (new ReadWriteShard (new JavaHashMapOfTreeMap))
  }

  def singleThreadShards (nlocks: Int, nshards: Int): ShardedTable = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedTable (LockSpace (nlocks), nshards - 1) (
      new SingleThreadShard (
        new JavaHashMapOfTreeMap,
        SingleThreadScheduler.usingSingleThreadExecutor))
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
