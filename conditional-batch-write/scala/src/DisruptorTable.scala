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
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit, TimeoutException}
import scala.util.Random

import com.lmax.disruptor.{EventFactory, EventHandler}
import com.lmax.disruptor.dsl.Disruptor

class TableEvent (nshards: Int) {

  import TableEvent._

  // We manually inlined two ops into separate fields for preallocation. The tests need only two;
  // for a live deployment we could inline more and use and array for spill.

  var op: Int = 0
  var n: Int = 0
  var t: Int = 0

  var k1: Int = 0
  var v1: Int = 0
  var t1: Int = 0

  var k2: Int = 0
  var v2: Int = 0
  var t2: Int = 0

  var cs: Array [Seq [Cell]] = new Array (nshards)

  val barrier = new CyclicBarrier (2)

  /** Fill the event for a read op. */
  def read (t: Int, ks: Seq [Int]) {
    val n = ks.size
    this.op = READ
    this.n = n
    this.t = t
    if (n > 0)
      this.k1 = ks (0)
    if (n > 1)
      this.k2 = ks (1)
  }

  /** Fill the event for a prepare op. */
  def prepare (rs: Seq [Row]) {
    val n = rs.size
    this.op = PREPARE
    this.n = n
    this.k1 = if (n > 0) rs (0) .k else 0
    this.k2 = if (n > 1) rs (1) .k else 0
  }

  /** Fill the event for a commit op. */
  def commit (t: Int, rs: Seq [Row]) {
    val n = rs.size
    this.op = COMMIT
    this.n = n
    this.t = t
    if (n > 0) {
      this.k1 = rs (0) .k
      this.v1 = rs (0) .v
    }
    if (n > 1) {
      this.k2 = rs (1) .k
      this.v2 = rs (1) .v
    }}

  /** Fill the event for a scan op. */
  def scan (t: Int) {
    this.op = SCAN
    this.t = t
  }

  /** Read the values result from the event. */
  def values: Seq [Value] = {
    val n = this.n
    val vs = new Array [Value] (n)
    if (n > 0)
      vs (0) = Value (v1, t1)
    if (n > 1)
      vs (1) = Value (v2, t2)
    vs.toSeq
  }

  /** Read the cells result from the event. */
  def cells: Seq [Cell] = {
    val cs = this.cs.flatten.toSeq
    for (i <- 0 until nshards)
      this.cs (i) = null
    cs
  }

  private def keys: Seq [Int] = {
    val n = this.n
    val ks = new Array [Int] (n)
    if (n > 0)
      ks (0) = k1
    if (n > 1)
      ks (1) = k2
    ks.toSeq
  }

  private def times: Seq [Int] = {
    val n = this.n
    val ts = new Array [Int] (n)
    if (n > 0)
      ts (0) = t1
    if (n > 1)
      ts (1) = t2
    ts.toSeq
  }

  private def rows: Seq [Row] = {
    val n = this.n
    val rs = new Array [Row] (n)
    if (n > 0)
      rs (0) = Row (k1, v1)
    if (n > 1)
      rs (1) = Row (k2, v2)
    rs.toSeq
  }

  override def toString: String =
    op match {
      case READ => s"TableEvent.Read($t, $keys => $values"
      case PREPARE => s"TableEvent.Prepare($t, $keys) => $times"
      case COMMIT => s"TableEvent.Commit($t, $rows)"
      case SCAN => s"TableEvent.Scan($t)"
    }
}

object TableEvent {

  val READ = 1
  val PREPARE = 2
  val COMMIT = 3
  val SCAN = 4
}

class TableFactory (nshards: Int) extends EventFactory [TableEvent] {

  def newInstance() = new TableEvent (nshards)
}

/** Run the op for keys that fall into this shard. */
class DisruptorShard (number: Int, mask: Int, shard: Shard) extends EventHandler [TableEvent] {

  import TableEvent._

  def onEvent (event: TableEvent, sequence: Long, endOfBatch: Boolean) {
    event.op match {
      case READ =>
        val n = event.n
        val t = event.t
        if (n > 0 && (event.k1 & mask) == number) {
          val v = shard.read (t, event.k1)
          event.v1 = v.v
          event.t1 = v.t
        }
        if (n > 1 && (event.k2 & mask) == number) {
          val v = shard.read (t, event.k2)
          event.v2 = v.v
          event.t2 = v.t
        }
      case PREPARE =>
        val n = event.n
        if (n > 0 && (event.k1 & mask) == number)
          event.t1 = shard.prepare (Row (event.k1, 0))
        if (n > 1 && (event.k2 & mask) == number)
          event.t2 = shard.prepare (Row (event.k2, 0))
      case COMMIT =>
        val n = event.n
        val t = event.t
        if (n > 0 && (event.k1 & mask) == number)
          shard.commit (t, Row (event.k1, event.v1))
        if (n > 1 && (event.k2 & mask) == number)
          shard.commit (t, Row (event.k2, event.v2))
      case SCAN =>
        event.cs (number) = shard.scan (event.t)
      case _ => ()
    }
  }

  override def toString = f"DisruptorShard($number, $mask%X)"
}

/** Wait for the shards to finish; notify the client and then wait for the client to finish. */
object EventAwait extends EventHandler [TableEvent] {

  import TableEvent._

  def onEvent (event: TableEvent, sequence: Long, endOfBatch: Boolean) {
    // The disruptor is configured to run this when the shards have completed.
    val t = event.op match {
      case READ => true
      case PREPARE => true
      case SCAN => true
      case _ => false
    }
    if (t) {
      try {
        // Notify the client.
        event.barrier.await (100, TimeUnit.MILLISECONDS)
        // Wait for the client to finish.
        event.barrier.await (100, TimeUnit.MILLISECONDS)
      } catch {
        case t: TimeoutException =>
          println (s"barrier timeout on $event")
      }
    }}}

class DisruptorTable (lock: LockSpace) (implicit params: Params) extends Table {

  import params.nshards

  require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")

  private val executor = Executors.newCachedThreadPool()
  private val disruptor = new Disruptor (new TableFactory (nshards), 1024, executor)
  private val ring = disruptor.getRingBuffer

  {
    val mask = nshards - 1
    val hs = Seq.tabulate (nshards) (new DisruptorShard (_, mask, new JavaHashMapOfTreeMap))
    disruptor.handleEventsWith (hs: _*)
    disruptor.after (hs: _*) .handleEventsWith (EventAwait)
    disruptor.start()
  }

  def time = lock.time

  def read (t: Int, ks: Int*): Seq [Value] = {
    // Queue the read, await the result.
    lock.read (t, ks)
    val n = ring.next()
    val event = ring.get (n)
    event.read (t, ks)
    ring.publish (n)
    event.barrier.await()
    try (event.values)
    finally (event.barrier.await())
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    // Queue the prepare, await the result.
    val n = ring.next()
    val event = ring.get (n)
    event.prepare (rs)
    ring.publish (n)
    try {
      event.barrier.await()
      val max = math.max (event.t1, event.t2)
      if (max > t) throw new StaleException (t, max)
    } finally {
      event.barrier.await()
    }}

  private def commit (t: Int, rs: Seq [Row]) {
    // Queue the commit; no need to wait.
    val n = ring.next()
    val event = ring.get (n)
    event.commit (t, rs)
    ring.publish (n)
  }

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
    // Queue the scan, await the result.
    val now = lock.time
    lock.scan (now)
    val n = ring.next()
    val event = ring.get (n)
    event.scan (now)
    ring.publish (n)
    event.barrier.await()
    try (event.cells)
    finally (event.barrier.await())
  }

  def close() {
    disruptor.shutdown()
    executor.shutdown()
  }}

trait NewDisruptorTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) = new DisruptorTable (AqsLock.newSpace)
}
