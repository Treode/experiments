package experiments

import org.scalatest.FlatSpec

class LockSpec extends FlatSpec {

  /** Run `action` in a new thread. */
  def thread (action: => Any): Thread = {
    val t = new Thread {
      override def run(): Unit =
        action
    }
    t.start()
    t
  }

  "A Lock" should "block a later writer" in {
    val lock = new Lock
    lock.write (1)
    thread {
      assert (lock.write (2) == 2)
    }
    lock.release (2)
  }

  it should "block a later writer and raise the time" in {
    val lock = new Lock
    lock.write (1)
    thread {
      assert (lock.write (2) == 3)
    }
    lock.release (3)
  }

  it should "block an earlier writer and raise the time" in {
    val lock = new Lock
    lock.write (2)
    thread {
      assert (lock.write (1) == 3)
    }
    lock.release (3)
  }

  it should "block a later reader" in {
    val lock = new Lock
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
    val lock = new Lock
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
    val lock = new Lock
    lock.write (1)
    val t = thread {
      lock.read (1)
    }
    t.join (10)
  }}

class LockSpaceSpec extends FlatSpec {

  def foreachMasked (mask: Int, ks: Int*): Seq [Int] = {
    val b = Seq.newBuilder [Int]
    LockSpace.foreach (LockSpace.maskKeys (mask, ks)) (b += _)
    b.result
  }

  "foreachMasked" should "work" in {
    assert (foreachMasked (1) == Seq.empty)
    assert (foreachMasked (1, 0) == Seq (0))
    assert (foreachMasked (1, 1) == Seq (1))
    assert (foreachMasked (1, 0, 2) == Seq (0))
    assert (foreachMasked (3, 0, 2) == Seq (0, 2))
    assert (foreachMasked (3, 2, 0) == Seq (0, 2))
  }}
