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

class StaleException (condition: Int, maximum: Int) extends Exception {

  override def getMessage: String =
    s"Stale write; condition: $condition, maximum: $maximum."
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

  /** A hint on how many buckets for hash tables. */
  val naccounts = 100

  /** Is this implementation safe to use from multiple threads? */
  def parallel: Boolean

  /** Make a table. */
  def newTable: Table
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

/** Wrap a table with a SingleThreadedExecutor to make it thread safe. */
class SingleThreadTable (table: Table) extends Table {

  private val executor = Executors.newSingleThreadExecutor

  private def submit [A] (f: => A): A =
    try {
      executor.submit (new Callable [A] {
        def call(): A = f
      }) .get
    } catch {
      case t: ExecutionException =>
        throw t.getCause
    }

  def time: Int =
    submit (table.time)

  def read (t: Int, ks: Int*): Seq [Value] =
    submit (table.read (t, ks: _*))

  def write (t: Int, rs: Row*): Int =
    submit (table.write (t, rs: _*))

  def scan(): Seq [Cell] =
    submit (table.scan())

  def close(): Unit =
    executor.shutdown()
}

/** A "naive" implementation of a thread-safe queue for a baseline. Performance in the context of
  * QueuedTable turns out to be similar to newSingleThreadExecutor, which we assume uses one of the
  * java.util.concurrent.FancyQueues.
  */
class SimpleQueue [A] {

  private val q = new ArrayDeque [A]

  def enqueue (v: A): Unit =
    synchronized {
      q.add (v)
      notify()
    }

  def dequeue(): A =
    synchronized {
      while (q.isEmpty)
        wait()
      q.remove()
    }}

/** Wrap a table with a work queeu to run all its operations in one thread. */
class QueuedTable (table: Table) extends Table {

  private val queue = new SimpleQueue [FutureTask [_]]

  private val thread = new Thread {
    override def run(): Unit =
      try {
        while (true)
          queue.dequeue().run()
      } catch {
        case t: InterruptedException => ()
      }}
  thread.start()

  private def submit [A] (func: => A): A =
    try {
      val task = new FutureTask (new Callable [A] {
        def call(): A = func
      })
      queue.enqueue (task)
      task.get
    } catch {
      case t: ExecutionException =>
        throw t.getCause
    }

  def time: Int =
    submit (table.time)

  def read (t: Int, ks: Int*): Seq [Value] =
    submit (table.read (t, ks: _*))

  def write (t: Int, rs: Row*): Int =
    submit (table.write (t, rs: _*))

  def scan(): Seq [Cell] =
    submit (table.scan())

  def close() {
    thread.interrupt()
    thread.join()
  }}

/** Methods to support functional and performance testing of the implementations. */
trait TableTools {
  this: NewTable =>

  val ntrials = 2000
  val nbrokers = 8
  val ntransfers = 1000

  /** Make a table, perform a method on it, close the table. */
  def withTable [A] (f: Table => A): A = {
    val table = newTable
    try (f (table)) finally (table.close())
  }

  /** Transfer money from one account to another `ntransfers` times in this thread. */
  def broker (table: Table) {

    val random = new Random
    var nstale = 0

    def transfer() {
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

    for (_ <- 0 until ntransfers)
      transfer()
    assert (nstale < ntransfers)
  }

  /** Transfer money from one account to another `ntransfers` times in a new thread. */
  class Broker (table: Table) extends Thread {
    override def run() = broker (table)
  }

  /** Run `nbrokers` brokers.
    * @param table The table that will track account balances.
    * @param parallel Run each broker in its own thread? If not, then run them serially in this
    * thread.
    */
  def transfers (table: Table, parallel: Boolean): Long = {
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

  def transfers (parallel: Boolean): Long =
    withTable (transfers (_, parallel))
}

 /** Repeat transfers experiments until 20 execute within 5% of the running mean time. */
trait TablePerf extends TableTools {
  this: NewTable =>

  val count = 20
  val tolerance = 0.05

  def perf() {

    println (getClass.getName)

    val M = 1000000.toDouble
    var sum = 0.toDouble
    var hits = count

    for (trial <- 0 until ntrials) {
      val ns = withTable (transfers (_, parallel)) .toDouble
      val ops = (nbrokers * ntransfers).toDouble
      val x = ops / ns * M
      sum += x
      val n = (trial + 1).toDouble
      val mean = sum / n
      val dev = math.abs (x - mean) / mean
      if (dev <= tolerance) {
        println (f"$trial%5d: $x%8.2f ops/ms ($mean%8.2f)")
        hits -= 1
        if (hits == 0)
          return
      }}}}
