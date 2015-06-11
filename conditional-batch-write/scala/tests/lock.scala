package experiments

import org.scalatest.FlatSpec

trait LockBehaviors extends FlatSpec {

  def newLock: Lock

  def repeat (test: => Any) {
    for (_ <- 0 until 100) {
      test
    }}

  def assertAlive (t: Thread) {
    Thread.`yield`()
    assert (t.isAlive)
  }

  def assertJoined (t: Thread) {
    t.join (100)
    assert (!t.isAlive)
  }

  /** Run `action` in a new thread. */
  def thread (action: => Any): Thread = {
    val t = new Thread {
      override def run(): Unit =
        action
    }
    t.start()
    t
  }

  "A lock" should "block a later writer" in repeat {
    val lock = newLock
    lock.write (1)
    val t = thread {
      assert (lock.write (2) == 2)
    }
    assertAlive (t)
    lock.release (2)
    assertJoined (t)
  }

  it should "block a later writer and raise the time" in repeat {
    val lock = newLock
    lock.write (1)
    val t = thread {
      assert (lock.write (2) == 3)
    }
    assertAlive (t)
    lock.release (3)
    assertJoined (t)
  }

  it should "block an earlier writer and raise the time" in repeat {
    val lock = newLock
    lock.write (2)
    val t = thread {
      assert (lock.write (1) == 3)
    }
    assertAlive (t)
    lock.release (3)
    assertJoined (t)
  }

  it should "block a later reader" in repeat {
    val lock = newLock
    lock.write (1)
    val t = thread {
      lock.read (2)
    }
    assertAlive (t)
    lock.release (2)
    assertJoined (t)
  }

  it should "block a later reader and raise the time" in repeat {
    val lock = newLock
    lock.write (1)
    val t = thread {
      lock.read (3)
    }
    assertAlive (t)
    lock.release (2)
    assertJoined (t)
    assert (lock.write (2) == 3)
  }

  it should "allow an earlier reader" in repeat {
    val lock = newLock
    lock.write (1)
    val t = thread {
      lock.read (1)
    }
    assertJoined (t)
  }}

class AqsLockSpec extends LockBehaviors {

  def newLock = new AqsLock
}

class ConditionLockSpec extends LockBehaviors {

  def newLock = new ConditionLock
}
