package experiments

import java.util.ArrayDeque
import java.util.concurrent._

/** Schedule tasks in one thread. */
trait SingleThreadScheduler {

  /** Call from any thread to schedule a task. */
  def execute (task: => Any)

  /** Call from any thread to schedule a task and await the result. */
  def submit [A] (task: => A): A

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

object SingleThreadScheduler {

  private class UsingSingleThreadExecutor extends SingleThreadScheduler {

    private val executor = Executors.newSingleThreadExecutor

    def execute (task: => Any): Unit =
      executor.submit (new Runnable {
        def run(): Unit = task
      })

    def submit [A] (task: => A): A =
      try {
        executor.submit (new Callable [A] {
          def call(): A = task
        }) .get
      } catch {
        case t: ExecutionException =>
          throw t.getCause
      }

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

    def submit [A] (task: => A): A =
      try {
        val future = new FutureTask (new Callable [A] {
          def call(): A = task
        })
        queue.enqueue (future)
        future.get
      } catch {
        case t: ExecutionException =>
          throw t.getCause
      }

    def shutdown(): Unit =
      thread.interrupt()
  }

  /** Use Java's SingleThreadExecutor for our SingleThreadScheduler. */
  def usingSingleThreadExecutor(): SingleThreadScheduler =
    new UsingSingleThreadExecutor

  def usingSimpleQueue(): SingleThreadScheduler =
    new UsingQueue (new SimpleQueue)

}
