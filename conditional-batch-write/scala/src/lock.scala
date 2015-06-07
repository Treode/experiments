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
import java.util.concurrent.atomic.AtomicInteger

/** A reader/writer lock that uses a logical clock.
  *
  * When a reader or writer acquires the lock, it supplies a timestamp, and that may raise the
  * logical time. The logical time is returned to a writer after it acquires the lock, and the
  * writer is obliged to commit writes using a timestamp strictly after that time. When the writer
  * releases the lock, it provides the write timestamp, and that may again raise the logical time.
  *
  * Only one writer at a time may hold the lock. Multiple readers may acquire the lock, some while
  * a writer holds it. Readers using a timestamp on or before the logical time will acquire the
  * lock immediately. Those using a timestamp strictly after the logical time will be blocked if a
  * writer holds the lock.
  *
  * This lock is useful with multiversion concurrency control. In a multiversion table, a reader
  * can get a value as-of some read time, even if there may be a concurrent writer, so long as that
  * writer uses a write time greater than the read time.
  */
trait Lock {

  def time: Int

  /** A reader wants to acquire the lock; the lock will ensure no future writer commits at or
    * before that timestamp.
    */
  def read (time: Int)

  /** A writer wants to acquire the lock; the lock provides the logical time, and the writer must
    * commit strictly after that time. The writer must eventuall call `release`.
    *
    * @param time The forecasted time; this may raise the logical time.
    * @return The logical time; the writer must commit using a timestamp strictly after this time.
    */
  def write (time: Int): Int

  /** A writer is finished with the lock.
    *
    * @param time The write time; this may raise the logical time.
    */
  def release (time: Int)
}

/** It's easy to shard the lock space. */
trait LockSpace {

  /** Track the maximum time seen from any lock. */
  def time: Int

  /** Acquire each identified lock. */
  def read (t: Int, ks: Seq [Int])

  /** Acquire each identified lock, in ascending order to avoid deadlock. */
  def write (t: Int, rs: Seq [Row]): Int

  /** Release each identified lock. */
  def release (t: Int, rs: Seq [Row])

  /** Acquire all locks for read. */
  def scan (t: Int)
}

object LockSpace {

  private class SingleLock (lock: Lock) extends LockSpace {

    def time = lock.time

    def read (t: Int, ks: Seq [Int]): Unit =
      lock.read (t)

    def write (t: Int, rs: Seq [Row]): Int =
      lock.write (t)

    def release (t: Int, ks: Seq [Row]): Unit =
      lock.release (t)

    def scan (t: Int): Unit =
      lock.read (t)
  }

  private class MultiLock (locks: Array [Lock]) extends LockSpace {

    private val mask = locks.size - 1
    private var clock = new AtomicInteger (0)

    private def raise (time: Int) {
      var now = clock.get
      while (now < time && !clock.compareAndSet (now, time))
        now = clock.get
    }

    def time = clock.get

    def read (t: Int, ks: Seq [Int]) {
      raise (t)
      val ns = maskKeys (mask, ks)
      foreach (ns) { n =>
        locks (n) .read (t)
      }}

    def write (t: Int, rs: Seq [Row]): Int = {
      val ns = maskRows (mask, rs)
      var max = -1
      foreach (ns) { n =>
        val ft = locks (n) .write (t)
        if (max < ft) max = ft
      }
      raise (max)
      max
    }

    def release (t: Int, rs: Seq [Row]) {
      raise (t)
      val ns = maskRows (mask, rs)
      foreach (ns) { n =>
        locks (n) .release (t)
      }}

    def scan (t: Int) {
      raise (t)
      var i = 0
      while (i < locks.length) {
        locks (i) .read (t)
        i += 1
      }}}

  def apply (nlocks: Int) (newLock: => Lock): LockSpace =
    if (nlocks == 1) {
      new SingleLock (newLock)
    } else {
      require (JInt.highestOneBit (nlocks) == nlocks, "nlocks must be a power of two")
      new MultiLock (Array.fill (nlocks) (newLock))
    }}