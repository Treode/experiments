package experiments

import java.lang.{Integer => JInt}
import java.util.ArrayDeque
import java.util.concurrent._

/** Schedule tasks in one thread. */
trait SingleThreadScheduler {

  /** Call from any thread to schedule a task. */
  def execute (task: => Any)

  /** Call from any thread to schedule a task and await the result. */
  def submit [A] (task: => A): Future [A]

  def shutdown()
}

/** A thread-safe queue. */
trait Queue [A] {

  def enqueue (v: A)
  def dequeue(): A
}

/** A "naive" implementation of a thread-safe queue for a baseline. */
class SimpleQueue [A] extends Queue [A] {

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

/** A queue that shards the input side to reduce contention. */
class ShardedQueue [A] private (mask: Int) extends Queue [A] {

  import ShardedQueue.{InputQueue, NonEmpty}

  private val nonEmpty = new NonEmpty (mask)
  private val in = Array.tabulate (mask + 1) (new InputQueue [A] (nonEmpty, _))
  private var out = new ArrayDeque [A]

  /** Use the thread id to choose the input queue. */
  private def id: Long =
    Thread.currentThread.getId

  def enqueue (v: A): Unit =
    // Enqueue it into the appropriate shard.
    in (id.toInt & mask) .enqueue (v)

  def dequeue(): A =
    // If we exhausted the output queue, await a non-empty input queue.
    synchronized {
      if (out.isEmpty) {
        val n = nonEmpty.await()
        out = in (n) .swap (out)
      }
      out.remove()
    }}

object ShardedQueue {

  /** Track if one or more queues are not empty. */
  class NonEmpty (mask: Int) {

    require (mask < 64, "Can track only 64 queues")

    private var bits = 0L
    private var next = mask

    /** Input `n` became non-empty. */
    def filled (n: Int): Unit =
      synchronized {
        val bit = 1L << n
        require ((bits & bit) == 0)
        bits |= bit
        notify()
      }

    /** Await the next non-empty queue and return its number. */
    def await(): Int =
      synchronized {
        while (bits == 0L)
          wait()
        while (true) {
          next = (next + 1) & mask
          val bit = 1L << next
          if ((bits & bit) > 0) {
            bits ^= bit
            return next
          }}
        return -1
      }}

  /** Swaps queues rather than dequeue. */
  class InputQueue [A] (nonEmpty: NonEmpty, n: Int) {

    private var q = new ArrayDeque [A]

    def enqueue (v: A): Unit =
      synchronized {
        val wasEmpty = q.isEmpty
        q.add (v)
        if (wasEmpty)
          nonEmpty.filled (n)
      }

    def swap (newq: ArrayDeque [A]): ArrayDeque [A] =
      synchronized {
        val oldq = q
        q = newq
        oldq
      }}

  def apply [A] (nshards: Int): ShardedQueue [A] = {
    require (JInt.highestOneBit (nshards) == nshards, "nshards must be a power of two")
    new ShardedQueue (nshards - 1)
  }}

object SingleThreadScheduler {

  private class UsingSingleThreadExecutor extends SingleThreadScheduler {

    private val executor = Executors.newSingleThreadExecutor

    def execute (task: => Any): Unit =
      executor.submit (new Runnable {
        def run(): Unit = task
      })

    def submit [A] (task: => A): Future [A] =
      executor.submit (new Callable [A] {
        def call(): A = task
      })

    def shutdown(): Unit =
      executor.shutdown()
  }

  private class UsingQueue (queue: Queue [Runnable]) extends SingleThreadScheduler {

    private val thread = new Thread {
      override def run(): Unit =
        try {
          while (true)
            queue.dequeue().run()
        } catch {
          case t: InterruptedException => ()
        }}
    thread.start()

    def execute (task: => Any): Unit =
      queue.enqueue (new Runnable {
        def run(): Unit = task
      })

    def submit [A] (task: => A): Future [A] = {
      val future = new FutureTask (new Callable [A] {
        def call(): A = task
      })
      queue.enqueue (future)
      future
    }

    def shutdown(): Unit =
      thread.interrupt()
  }

  /** Use Java's SingleThreadExecutor for our SingleThreadScheduler. */
  def newUsingExecutor: SingleThreadScheduler =
    new UsingSingleThreadExecutor

  def newUsingSimpleQueue: SingleThreadScheduler =
    new UsingQueue (new SimpleQueue)

  def newUsingShardedQueue (nshards: Int): SingleThreadScheduler =
    new UsingQueue (ShardedQueue (nshards))
}
