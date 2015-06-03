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

import java.util.concurrent.locks.AbstractQueuedSynchronizer

/** A Lock that uses Java's AbstractQueuedSynchronizer. */
class AqsLock extends Lock {

  private val sync = new AqsLock.Sync

  def time: Int =
    sync.getTime

  def read (time: Int) {
    sync.acquireShared (makeState (time, false))
    // Allow next parked thread to also proceed.
    sync.releaseShared (0)
  }

  def write (time: Int): Int = {
    sync.acquire (makeState (time, true))
    return sync.getTime
  }

  def release (time: Int): Unit =
    sync.release (makeState (time, false))

  override def toString: String =
    s"Lock(${sync.getTime})"
}

object AqsLock {

  /** See comment on the field Lock.sync. */
  private [AqsLock] class Sync extends AbstractQueuedSynchronizer {

    def getTime: Int =
      experiments.getTime (getState)

    override def isHeldExclusively(): Boolean =
      // The lowest bit indicates if a writer holds the lock.
      isHeld (getState)

    /** A writer wants to acquire the lock. If the lock is not held by another writer, raise the
      * logical time and grant the lock.
      */
    override def tryAcquire (forecast: Int): Boolean = {
      // The lock class should set this up for us.
      assert (isHeld (forecast))
      while (true) {
        val state = getState
        if (isHeld (state))
          return false
        // Use the greater of our forecast or state.
        val next = if (state <= forecast) forecast else setHeld (state, true)
        if (compareAndSetState (state, next)) {
          setExclusiveOwnerThread (Thread.currentThread)
          return true
        }}
      return false
    }

    override def tryRelease (forecast: Int): Boolean = {
      assert (!isHeld (forecast))
      while (true) {
        val state = getState
        // Use the greater of our forecast or state.
        val next = if (state <= forecast) forecast else setHeld (state, false)
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
      assert (!isHeld (forecast))
      while (true) {
        val state = getState
        if (forecast <= state)
          return 1;
        if (isHeld (state))
          return -1;
        if (compareAndSetState (state, forecast))
          return 1
      }
      return -1
    }

    override def tryReleaseShared (ignored: Int): Boolean =
      true
  }

  def space (nlocks: Int): LockSpace =
    LockSpace (nlocks) (new AqsLock)
}
