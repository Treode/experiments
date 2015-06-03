package experiments

import org.scalatest.FlatSpec

trait LockBehaviors extends FlatSpec {

  def newLock: Lock

  /** Run `action` in a new thread. */
  def thread (action: => Any): Thread = {
    val t = new Thread {
      override def run(): Unit =
        action
    }
    t.start()
    t
  }

  "A lock" should "block a later writer" in {
    val lock = newLock
    lock.write (1)
    thread {
      assert (lock.write (2) == 2)
    }
    lock.release (2)
  }

  it should "block a later writer and raise the time" in {
    val lock = newLock
    lock.write (1)
    thread {
      assert (lock.write (2) == 3)
    }
    lock.release (3)
  }

  it should "block an earlier writer and raise the time" in {
    val lock = newLock
    lock.write (2)
    thread {
      assert (lock.write (1) == 3)
    }
    lock.release (3)
  }

  it should "block a later reader" in {
    val lock = newLock
    @volatile var p = false
    lock.write (1)
    val t = thread {
      lock.read (2)
      p = true
    }
    Thread.sleep (10)
    assert (!p)
    lock.release (2)
    t.join (10)
  }

  it should "block a later reader and raise the time" in {
    val lock = newLock
    @volatile var p = false
    lock.write (1)
    val t = thread {
      lock.read (3)
      p = true
    }
    Thread.sleep (10)
    assert (!p)
    lock.release (2)
    t.join (10)
    assert (lock.write (2) == 3)
  }

  it should "allow an earlier reader" in {
    val lock = newLock
    lock.write (1)
    val t = thread {
      lock.read (1)
    }
    t.join (10)
  }}

class AqsLockSpec extends LockBehaviors {

  def newLock = new AqsLock
}
