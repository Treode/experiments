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
import java.util.ArrayDeque
import java.util.concurrent._

import org.jctools.queues.MpscCompoundQueue

/** Schedule tasks and eventually execute them. */
trait Scheduler {

  /** Call from any thread to schedule a task. */
  def execute (task: Runnable)

  /** Call from any thread to schedule a task. */
  def execute (task: => Any)

  def shutdown()
}

object Scheduler {

  private class UsingExecutor (executor: ExecutorService) extends Scheduler {

    def execute (task: Runnable): Unit =
      executor.execute (new Runnable {
        def run(): Unit =
          try {
            task.run()
          } catch {
            case t: Throwable =>
              println (s"uncaught $t")
              executor.shutdown()
          }})

    def execute (task: => Any): Unit =
      executor.execute (new Runnable {
        def run(): Unit =
          try {
            task
          } catch {
            case t: Throwable =>
              println (s"uncaught $t")
              executor.shutdown()
          }})

    def shutdown(): Unit =
      executor.shutdown()
  }

  /** Use Java's cached thread pool for our Scheduler. */
  def newUsingCachedThreadPool: Scheduler =
    new UsingExecutor (Executors.newCachedThreadPool)

  /** Use Java's ForkJoinPool for our Scheduler. */
  def newUsingForkJoinPool: Scheduler =
    new UsingExecutor (new ForkJoinPool (64))
}

/** Schedule tasks and eventually execute them in one thread. */
trait SingleThreadScheduler extends Scheduler {

  /** Call from any thread to schedule a task and await the result. */
  def submit [A] (task: => A): Future [A]
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

class JCToolsQueue [A] (capacity: Int) extends Queue [A] {

  private val q = new MpscCompoundQueue [A] (capacity)

  def enqueue (v: A) {
    var n = 3
    var r = q.offer (v)
    while (!r && n > 0) {
      n -= 1
      r = q.offer (v)
    }
    while (!r) {
      Thread.sleep (0, 1)
      r = q.offer (v)
    }}

  def dequeue(): A = {
    var n = 3000
    var v = q.poll()
    while (v == null && n > 0) {
      n -= 1
      v = q.poll()
    }
    while (v == null) {
      Thread.sleep (0, 1)
      v = q.poll()
    }
    v
  }}

object SingleThreadScheduler {

  private class UsingSingleThreadExecutor extends SingleThreadScheduler {

    private val executor = Executors.newSingleThreadExecutor

    def execute (task: Runnable): Unit =
      executor.execute (task)

    def execute (task: => Any): Unit =
      executor.execute (new Runnable {
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

    def execute (task: Runnable): Unit =
      queue.enqueue (task)

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

  def newUsingJCToolsQueue: SingleThreadScheduler =
    new UsingQueue (new JCToolsQueue (1024))
}

class Fiber (scheduler: Scheduler) extends Scheduler {

  /** Treated as a constant. */
  private [this] val empty = new ArrayDeque [Runnable]

  private [this] var tasks = new ArrayDeque [Runnable]
  private [this] var engaged = false

  /** If engaged, enqueue the task and return false. Otherwise, return true to indicate caller
    * can run the task immediately in this thread.
    */
  private [this] def enqueue (task: Runnable): Boolean =
    synchronized {
      if (!engaged) {
        engaged = true
        false
      } else {
        tasks.add (task)
        true
      }}

  /** Return the current queue of tasks. */
  private [this] def drain(): ArrayDeque [Runnable] =
    synchronized {
      if (tasks.isEmpty) {
        engaged = false
        empty
      } else {
        val t = tasks
        tasks = new ArrayDeque
        t
      }}

  private [this] def add (task: Runnable): Unit = {
    if (!enqueue (task)) {
      task.run()
      var next = drain()
      while (!next.isEmpty) {
        while (!next.isEmpty)
          next.remove().run()
        next = drain()
      }}}

  def execute (task: Runnable): Unit =
    add (task)

  def execute (task: => Any): Unit =
    add (new Runnable {
      def run(): Unit = task
    })

  def shutdown() = ()
}
