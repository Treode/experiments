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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

trait AsyncTable {

  def time: Int

  /** Read keys `ks` as of time `t`. */
  def read (t: Int, ks: Int*) (cb: Seq [Value] => Any)

  /** Write rows `rs` if they haven't changed since time `t`. */
  def write (t: Int, rs: Row*) (cb: Either [Int, StaleException] => Any)

  /** Scan the entire history. This is not part of the performance timings. */
  def scan (cb: Seq [Cell] => Any)
}

trait NewAsyncTable {

  def newScheduler: Scheduler

  def newTable (implicit params: Params, scheduler: Scheduler): AsyncTable
}

trait AsyncTableTools {
  this: NewAsyncTable =>

  /** Make a table, perform a method on it, close the table. */
  def withTable [A] (f: AsyncTable => Scheduler => A) (implicit params: Params): A = {
    implicit val scheduler = newScheduler
    val table = newTable
    try (f (table) (scheduler)) finally (scheduler.shutdown())
  }

  def broker (table: AsyncTable) (cb: Unit => Any) (implicit params: Params, scheduler: Scheduler) {

    import params.{naccounts, ntransfers}
    val random = new Random
    var nstale = 0
    var itransfer = 0

    def transfer() {
      // Two accounts and an amount to transfer from a1 to a2
      val a1 = random.nextInt (naccounts)
      var a2 = random.nextInt (naccounts)
      while (a2 == a1) a2 = random.nextInt (naccounts)
      val n = random.nextInt (1000)

      // Do the transfer
      val rt = table.time
      table.read (rt, a1, a2) { case Seq (v1, v2) =>
        table.write (rt, Row (a1, v1.v - n), Row (a2, v2.v + n)) { r =>
          r match {
            case Right (_) => nstale += 1
            case _ => ()
          }
          itransfer += 1
          if (itransfer < ntransfers) {
            scheduler.execute (transfer())
          } else {
            assert (nstale < ntransfers)
            cb (())
          }}}}

    transfer()
  }

  def transfers (table: AsyncTable) (implicit params: Params, scheduler: Scheduler): Long = {
    import params.nbrokers
    val finished = new CountDownLatch (nbrokers)
    val start = System.nanoTime
    for (_ <- 0 until nbrokers)
      scheduler.execute {
        broker (table) { _ =>
          finished.countDown()
        }}
    assert (finished.await (10, SECONDS))
    val end = System.nanoTime
    end - start
  }
}

class FiberizedTable (table: Table) (implicit scheduler: Scheduler) extends AsyncTable {

  private val fiber = new Fiber (scheduler)

  def time: Int =
    table.time

  def read (t: Int, ks: Int*) (cb: Seq [Value] => Any): Unit = {
    fiber.execute {
      val vs = table.read (t, ks: _*)
      cb (vs)
    }
  }

  def write (t: Int, rs: Row*) (cb: Either [Int, StaleException] => Any): Unit =
    fiber.execute {
      val result =
        try {
          Left (table.write (t, rs: _*))
        } catch {
          case t: StaleException => Right (t)
        }
      cb (result)
    }

  def scan (cb: Seq [Cell] => Any): Unit =
    fiber.execute {
      val cs = table.scan();
      scheduler.execute (cb (cs))
    }}

trait NewFiberizedTable extends NewAsyncTable {

  def newScheduler = Scheduler.newUsingCachedThreadPool

  def newTable (implicit params: Params, scheduler: Scheduler) =
    new FiberizedTable (new JavaHashMapOfTreeMap)
}

trait NewFiberizedForkJoinTable extends NewAsyncTable {

  def newScheduler = Scheduler.newUsingForkJoinPool

  def newTable (implicit params: Params, scheduler: Scheduler) =
    new FiberizedTable (new JavaHashMapOfTreeMap)
}

class AsyncLock (implicit scheduler: Scheduler) {

  private var clock: Int = 0
  private var future: Int = 0
  private var readers = List.empty [Unit => Any]
  private var writers = List.empty [Int => Any]
  private var held: Boolean = false

  def time: Int =
    synchronized (clock)

  def read (t: Int) (cb: Unit => Any) {
    val r =
      synchronized {
        if (t <= clock) {
          true
        } else if (!held) {
          clock = t;
          true
        } else {
          future = t;
          readers ::= cb
          false
        }}
    if (r)
      scheduler.execute (cb (()))
  }

  def write (t: Int) (cb: Int => Any) {
    val r: Option [Int] =
      synchronized {
        if (!held) {
          held = true
          Some (clock)
        } else {
          if (future < t)
            future = t
          writers ::= cb
          None
        }}
    if (r.isDefined)
      scheduler.execute (cb (r.get))
  }

  def release (t: Int) {
    val (rs, w, t2) =
      synchronized {
        if (clock < t)
          clock = t
        if (clock < future)
          clock = future
        val rs: List [Unit => Any] =
          if (readers.isEmpty) {
            List.empty
          } else {
            val _rs = readers
            readers = List.empty
            _rs
          }
        val w =
          if (writers.isEmpty) {
            held = false
            None
          } else {
            val _w = writers.head
            writers = writers.tail
            Some (_w)
          }
        (rs, w, clock)
      }
    for (cb <- rs)
      scheduler.execute (cb (()))
    for (cb <- w)
      scheduler.execute (cb (t2))
  }}

trait AsyncLockSpace {

  def time: Int

  def read (t: Int, ks: Seq [Int]) (cb: Unit => Any)

  def write (t: Int, rs: Seq [Row]) (cb: Int => Any)

  def release (t: Int, rs: Seq [Row])

  def scan (cb: Int => Any)
}

object AsyncLockSpace {

  private class SingleLock (implicit scheduler: Scheduler) extends AsyncLockSpace {

    private var lock = new AsyncLock

    def time: Int =
      lock.time

    def read (t: Int, ks: Seq [Int]) (cb: Unit => Any): Unit =
      lock.read (t) (cb)

    def write (t: Int, rs: Seq [Row]) (cb: Int => Any): Unit =
      lock.write (t) (cb)

    def release (t: Int, rs: Seq [Row]): Unit =
      lock.release (t)

    def scan (cb: Int => Any) {
      val t = lock.time
      lock.read (t) (_ => cb (t))
    }}

  private class MultiLock (mask: Int) (implicit scheduler: Scheduler) extends AsyncLockSpace {

    private var locks = Array.fill (mask + 1) (new AsyncLock)
    private var clock = new AtomicInteger (0)

    private def raise (time: Int) {
      var now = clock.get
      while (now < time && !clock.compareAndSet (now, time))
        now = clock.get
    }

    def time: Int =
      clock.get

    def read (t: Int, ks: Seq [Int]) (cb: Unit => Any) {
      raise (t)
      val ns = maskKeys (mask, ks)
      var i = 0
      var n = -1
      def loop (x: Unit) {
        while (i < ns.length && ns (i) == n)
          i += 1
        if (i < ns.length) {
          val m = ns (i)
          i += 1
          n = m
          locks (m) .read (t) (loop _)
        } else {
          scheduler.execute (cb (()))
        }}
      loop (())
    }

    def write (t: Int, rs: Seq [Row]) (cb: Int => Any) {
      raise (t)
      val ns = maskRows (mask, rs)
      var i = 0
      var n = -1
      var max = 0
      def loop (t2: Int) {
        if (max < t2)
          max = t2
        while (i < ns.length && ns (i) == n)
          i += 1
        if (i < ns.length) {
          val m = ns (i)
          i += 1
          n = m
          locks (m) .write (t) (loop _)
        } else {
          scheduler.execute (cb (max))
        }}
      loop (0)
    }

    def release (t: Int, rs: Seq [Row]) {
      raise (t)
      val ns = maskRows (mask, rs)
      foreach (ns) { n =>
        locks (n) .release (t)
      }}

    def scan (cb: Int => Any) {
      val t = clock.get
      var i = 0
      def loop (x: Unit) {
        if (i < locks.length) {
          i += 1
          locks (i-1) .read (t) (loop _)
        } else {
          scheduler.execute (cb (t))
        }}
      loop (())
    }}

  def apply () (implicit params: Params, scheduler: Scheduler): AsyncLockSpace = {
    import params.nlocks
    if (nlocks == 1) {
      new SingleLock
    } else {
      require (JInt.highestOneBit (nlocks) == nlocks, "nlocks must be a power of two")
      new MultiLock (nlocks - 1)
    }}}

trait AsyncShard {

  def read (t: Int, k: Int) (cb: Value => Any)

  def prepare (r: Row) (cb: Int => Any)

  def commit (t: Int, r: Row) (cb: Unit => Any)

  def scan (t: Int) (cb: Seq [Cell] => Any)
}

class FiberizedShard (shard: Shard) (implicit scheduler: Scheduler) extends AsyncShard {

  private val fiber = new Fiber (scheduler)

  def read (t: Int, k: Int) (cb: Value => Any): Unit =
    fiber.execute {
      val v = shard.read (t, k)
      cb (v)
    }

  def prepare (r: Row) (cb: Int => Any): Unit =
    fiber.execute {
      val t = shard.prepare (r)
      cb (t)
    }

  def commit (t: Int, r: Row) (cb: Unit => Any): Unit =
    fiber.execute {
      shard.commit (t, r)
      cb (())
    }

  def scan (t: Int) (cb: Seq [Cell] => Any): Unit =
    fiber.execute {
      val cs = shard.scan (t)
      scheduler.execute (cb (cs))
    }
}

class AsyncShardedTable (
  mask: Int
) (
  newShard: => AsyncShard
) (implicit
  params: Params,
  scheduler: Scheduler
) extends AsyncTable {

  private val lock = AsyncLockSpace()
  private val shards = Array.fill (mask + 1) (newShard)

  def time: Int =
    lock.time

  private def read (t: Int, k: Int) (cb: Value => Any): Unit =
    shards (k & mask) .read (t, k) (cb)

  def read (t: Int, ks: Int*) (cb: Seq [Value] => Any) {
    lock.read (t, ks) { _ =>
      val i = ks.iterator
      val b = Seq.newBuilder [Value]
      def loop() {
        if (i.hasNext) {
          val k = i.next
          read (t, k) { v =>
            b += v
            loop()
          }
        } else {
          scheduler.execute (cb (b.result))
        }}
      loop()
    }}

  private def prepare (r: Row) (cb: Int => Any): Unit =
    shards (r.k & mask) .prepare (r) (cb)

  private def prepare (rs: Seq [Row]) (cb: Int => Any) {
    val i = rs.iterator
    var max = 0
    def loop() {
      if (i.hasNext) {
        val r = i.next
        prepare (r) { t =>
          if (max < t)
            max = t
          loop()
        }
      } else {
        scheduler.execute (cb (max))
      }}
    loop()
  }

  private def commit (t: Int, r: Row) (cb: Unit => Any): Unit =
    shards (r.k & mask) .commit (t, r) (cb)

  private def commit (t: Int, rs: Seq [Row]) (cb: Unit => Any): Unit = {
    val i = rs.iterator
    var max = 0
    def loop() {
      if (i.hasNext) {
        val r = i.next
        commit (t, r) { _ =>
          loop()
        }
      } else {
        scheduler.execute (cb (max))
      }}
    loop()
  }

  def write (t: Int, rs: Row*) (cb: Either [Int, StaleException] => Any) {
    lock.write (t, rs) { _wt =>
      val wt = _wt + 1
      prepare (rs) { max =>
        if (max > t) {
          lock.release (wt, rs)
          cb (Right (new StaleException (t, max)))
        } else {
          commit (wt, rs) { _ =>
            lock.release (wt, rs)
            cb (Left (wt))
          }}}}}

  def scan (cb: Seq [Cell] => Any) {
    lock.scan { t =>
      val i = shards.iterator
      val cs = Seq.newBuilder [Cell]
      def loop() {
        if (i.hasNext) {
          val s = i.next
          s.scan (t) { _cs =>
            cs ++= _cs
            loop()
          }
        } else {
          scheduler.execute (cb (cs.result))
        }}
      loop()
    }}}

object AsyncShardedTable {

  def apply (newShard: => AsyncShard) (implicit params: Params, scheduler: Scheduler): AsyncShardedTable = {
    import params.nshards
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new AsyncShardedTable (nshards - 1) (newShard)
  }}

trait NewFiberizedShardedTable extends NewAsyncTable {

  def newScheduler = Scheduler.newUsingCachedThreadPool

  def newTable (implicit params: Params, scheduler: Scheduler) =
    AsyncShardedTable (new FiberizedShard (new JavaHashMapOfTreeMap))
}

trait NewFiberizedShardedForkJoinTable extends NewAsyncTable {

  def newScheduler = Scheduler.newUsingForkJoinPool

  def newTable (implicit params: Params, scheduler: Scheduler) =
    AsyncShardedTable (new FiberizedShard (new JavaHashMapOfTreeMap))
}
