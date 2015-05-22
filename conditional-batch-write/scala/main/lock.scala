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
import java.util.Arrays
import java.util.concurrent.locks.AbstractQueuedSynchronizer

import org.scalatest.FlatSpec

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
private class Lock {

  /** The state of the `AbstractQueuedSynchronizer` is
    * {{{
    * (logical_time << 1) | (exclusively_held)
    * }}}
    * It encodes both the logical time and whether or not a writer holds the lock. Both the Sync
    * instance and this Lock instance work with the form the state.
    */
  private val sync = new Lock.Sync

  def time: Int =
    sync.getTime

  /** A reader wants to acquire the lock; the lock will ensure no future writer commits at or
    * before that timestamp.
    */
  def read (time: Int): Unit =
    sync.acquireShared (time << 1)

  /** A writer wants to acquire the lock; the lock provides the logical time, and the writer must
    * commit strictly after that time. The writer must eventuall call `release`.
    *
    * @param time The forecasted time; this may raise the logical time.
    * @return The logical time; the writer must commit using a timestamp strictly after this time.
    */
  def write (time: Int): Int = {
    sync.acquire ((time << 1) | 1)
    return sync.getTime
  }

  /** A writer is finished with the lock.
    *
    * @param time The write time; this may raise the logical time.
    */
  def release (time: Int): Unit =
    sync.release ((time << 1) & (-1 << 1))

  override def toString: String =
    s"Lock(${sync.getTime})"
}

object Lock {

  /** See comment on the field Lock.sync. */
  private [Lock] class Sync extends AbstractQueuedSynchronizer {

    def getTime: Int =
      // The time is in the upper 63 bits.
      getState >>> 1

    override def isHeldExclusively(): Boolean =
      // The lowest bit indicates if a writer holds the lock.
      (getState & 1) == 1

    /** A writer wants to acquire the lock. If the lock is not held by another writer, raise the
      * logical time and grant the lock.
      */
    override def tryAcquire (forecast: Int): Boolean = {
      // The lock class should set this up for us.
      assert ((forecast & 1) == 1)
      while (true) {
        val state = getState
        val held = (state & 1) == 1
        // If held, then loop.
        if (!held) {
          // Use the greater of our forecast or state.
          val next = if (state <= forecast) forecast else (state | 1)
          if (compareAndSetState (state, next)) {
            setExclusiveOwnerThread (Thread.currentThread)
            return true
          }}}
      return false
    }

    override def tryRelease (forecast: Int): Boolean = {
      assert ((forecast & 1) == 0)
      while (true) {
        val state = getState
        // Use the greater of our forecast or state.
        val next = if (state <= forecast) forecast else (state & (-1 << 1))
        if (compareAndSetState (state, next)) {
          // We WIN!
          setExclusiveOwnerThread (null)
          return true
        }}
      return false
    }

    /** A reader wants to acquire the lock.
      *
      * If the reader's timestamp is
      *
      * - less than or equal to the logical time, grant the lock.
      *
      * - greater than the logical time and the lock is free, raise the logical time and grant the
      *   lock.
      *
      * - greater than the logical time and a writer holds the lock, then block the reader.
      */
    override def tryAcquireShared (forecast: Int): Int = {
      // The lock class should set this up for us.
      assert ((forecast & 1) == 0)
      while (true) {
        val state = getState
        val held = (state & 1) == 1
        // If held by a writer that may write before our forecasted time, then loop.
        if (!held || forecast < state) {
          // Use the greater of our forecast or state.
          val next = if (state <= forecast) forecast else state
          if (compareAndSetState (state, next)) {
            return 1
          }}}
      return -1
    }}}

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

  private class SingleLock extends LockSpace {

    private val lock = new Lock

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

  // Visible for testing.
  def maskKeys (mask: Int, ks: Seq [Int]): Array [Int] = {
    val ns = new Array [Int] (ks.length)
    var i = 0
    while (i < ks.length) {
      ns (i) = ks (i) & mask
      i += 1
    }
    Arrays.sort (ns)
    ns
  }

  def maskRows (mask: Int, rs: Seq [Row]): Array [Int] = {
    val ns = new Array [Int] (rs.length)
    var i = 0
    while (i < rs.length) {
      ns (i) = rs (i) .k & mask
      i += 1
    }
    Arrays.sort (ns)
    ns
  }

  def foreach (ns: Seq [Int]) (f: Int => Any) {
    var i = 0
    var n = -1
    while (i < ns.length) {
      val m = ns (i)
      if (m > n) {
        f (m)
        n = m
      }
      i += 1
    }}

  private class MultiLock (mask: Int) extends LockSpace {

    private val locks = Array.fill (mask + 1) (new Lock)

    private var _time = 0

    private def raise (t: Int): Unit =
      synchronized {
        if (_time < t) _time = t
      }

    def time = _time

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

  def apply (nlocks: Int): LockSpace =
    if (nlocks == 1) {
      new SingleLock
    } else {
      require (JInt.highestOneBit (nlocks) == nlocks, "nlocks must be a power of two")
      new MultiLock (nlocks - 1)
    }}
