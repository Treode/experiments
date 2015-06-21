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

import java.util.ArrayDeque
import java.util.concurrent._
import scala.util.Random

import org.scalatest.FlatSpec

/** Key `k` as of time `t`; sorted by key and reverse time. */
case class Key (k: Int, t: Int) extends Ordered [Key] {

  def compare (that: Key): Int = {
    val r = k compare that.k
    if (r != 0) return r
    that.t compare t
  }}

object Key extends Ordering [Key] {

  def compare (x: Key, y: Key): Int =
    x compare y
}

/** Value `v` as of time `t`. */
case class Value (v: Int, t: Int)

object Value {

  val empty = Value (0, 0)
}

/** Value `v` for key `k`. */
case class Row (k: Int, v: Int)

/** Value `v` for key `k` as of time `t`; sorted by key and (not reverse) time. */
case class Cell (k: Int, v: Int, t: Int) extends Ordered [Cell] {

  def compare (that: Cell): Int = {
    val r = k compare that.k
    if (r != 0) return r
    t compare that.t
  }}

object Cell extends Ordering [Cell] {

  def compare (x: Cell, y: Cell): Int =
    x compare y
}

class StaleException (cond: Int, max: Int) extends Exception {

  override def getMessage: String =
    s"Stale write; cond: $cond, max: $max."
}

/** A table (or key-value table, hash table) using conditional batch write. Keys and values are
  * integers to keep this simple, because our focus is on comparing performance of different
  * implementation options.
  */
trait Table {

  def time: Int

  /** Read keys `ks` as of time `t`. */
  def read (t: Int, ks: Int*): Seq [Value]

  /** Write rows `rs` if they haven't changed since time `t`. */
  def write (t: Int, rs: Row*): Int

  /** Scan the entire history. This is not part of the performance timings. */
  def scan(): Seq [Cell]

  def close()
}

/** Factory to make tables. */
trait NewTable {

  /** Is this implementation safe to use from multiple threads? */
  def parallel: Boolean

  /** Make a table. */
  def newTable (implicit params: Params): Table

  def newRecommendedTable (implicit params: Params): Table =
    new TableFromShard (new JavaHashMapOfTreeMap)

  def newRecommendedShard (implicit params: Params): Shard =
    new JavaHashMapOfTreeMap

  def newRecommendedScheduler: SingleThreadScheduler =
    SingleThreadScheduler.newUsingExecutor
}

/** Wrap a table with synchronized to make it thread safe. */
class SynchronizedTable (table: Table) extends Table {

  def time: Int =
    synchronized (table.time)

  def read (t: Int, ks: Int*): Seq [Value] =
    synchronized (table.read (t, ks: _*))

  def write (t: Int, rs: Row*): Int =
    synchronized (table.write (t, rs: _*))

  def scan(): Seq [Cell] =
    synchronized (table.scan())

  def close(): Unit =
    synchronized (table.close())
}

trait NewSynchronizedTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    new SynchronizedTable (newRecommendedTable)
}

/** Wrap a table with a `SingleThreadScheduler` to make it thread safe. */
class SingleThreadTable (table: Table, scheduler: SingleThreadScheduler) extends Table {

  def time: Int =
    scheduler.submit (table.time) .safeGet

  def read (t: Int, ks: Int*): Seq [Value] =
    scheduler.submit (table.read (t, ks: _*)) .safeGet

  def write (t: Int, rs: Row*): Int =
    scheduler.submit (table.write (t, rs: _*)) .safeGet

  def scan(): Seq [Cell] =
    scheduler.submit (table.scan()) .safeGet

  def close(): Unit =
    scheduler.shutdown()
}

trait NewSingleThreadExecutorTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    new SingleThreadTable (
      newRecommendedTable,
      SingleThreadScheduler.newUsingExecutor)
}

trait NewSimpleQueueTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    new SingleThreadTable (
      newRecommendedTable,
      SingleThreadScheduler.newUsingSimpleQueue)
}

trait NewShardedQueueTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    new SingleThreadTable (
      newRecommendedTable,
      SingleThreadScheduler.newUsingShardedQueue (params.nshards))
}

trait NewJCToolsQueueTable extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) =
    new SingleThreadTable (
      newRecommendedTable,
      SingleThreadScheduler.newUsingJCToolsQueue)
}

/** Methods to support functional and performance testing of the implementations. */
trait TableTools {
  this: NewTable =>

  /** Make a table, perform a method on it, close the table. */
  def withTable [A] (f: Table => A) (implicit params: Params): A = {
    val table = newTable
    try (f (table)) finally (table.close())
  }

  /** Transfer money from one account to another `ntransfers` times in this thread. */
  def broker (table: Table) (implicit params: Params) {
    import params.{naccounts, nbrokers, ntransfers}
    val random = new Random
    var nstale = 0

    def transfer () {
      // Two accounts and an amount to transfer from a1 to a2
      val a1 = random.nextInt (naccounts)
      var a2 = random.nextInt (naccounts)
      while (a2 == a1) a2 = random.nextInt (naccounts)
      val n = random.nextInt (1000)

      // Do the transfer
      val rt = table.time
      val Seq (v1, v2) = table.read (rt, a1, a2)
      try {
        table.write (rt, Row (a1, v1.v - n), Row (a2, v2.v + n))
      } catch {
        case t: StaleException => nstale += 1
      }}

    val count = ntransfers / nbrokers
    for (_ <- 0 until count)
      transfer()
    assert (nstale < count)
  }

  /** Transfer money from one account to another `ntransfers` times in a new thread. */
  class Broker (table: Table) (implicit params: Params) extends Thread {
    override def run() = broker (table)
  }

  /** Run `nbrokers` brokers.
    * @param table The table that will track account balances.
    * @param parallel Run each broker in its own thread? If not, then run them serially in this
    * thread.
    */
  def transfers (table: Table, parallel: Boolean) (implicit params: Params): Long = {
    import params.nbrokers
    if (parallel) {
      val ts = for (_ <- 0 until nbrokers) yield new Broker (table)
      val start = System.nanoTime
      ts foreach (_.start())
      ts foreach (_.join())
      val end = System.nanoTime
      end - start
    } else {
      val start = System.nanoTime
      for (_ <- 0 until nbrokers)
        broker (table)
      val end = System.nanoTime
      end - start
    }}

  def transfers (parallel: Boolean) (implicit params: Params): Long =
    withTable (transfers (_, parallel))
}
