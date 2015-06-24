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

import java.util.{ArrayDeque, ArrayList}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConversions._

/** A lock that uses ReentrantLock and its condition queues. */
class ConditionLock extends Lock {

  import ConditionLock.Waiter

  private val lock = new ReentrantLock
  private val readers = lock.newCondition
  private val writers = lock.newCondition
  private var state = 0
  private var future = 0

  def time: Int = {
    lock.lock()
    try {
      getTime (state)
    } finally {
      lock.unlock()
    }
  }

  def read_acquire (time: Int) {
    lock.lock()
    try {
      while (true) {
        if (time <= getTime (state))
          return
        if (!isHeld (state)) {
          state = makeState (time, false)
          return
        }
        if (future < time)
          future = time
        readers.await()
      }
    } catch {
      case t: Throwable =>
        lock.unlock();
        throw t;
    }
  }

  def read_release(): Unit =
    lock.unlock()

  def read (time: Int) {
    read_acquire (time)
    read_release();
  }

  def write_acquire (time: Int): Int = {
    lock.lock()
    try {
      while (isHeld (state))
        writers.await()
      var now = getTime (state)
      if (now < time)
        now = time
      state = makeState (now, true)
      now
    } catch {
      case t: Throwable =>
        lock.unlock()
        throw t
    }
  }

  def write_release(): Unit =
    lock.unlock()

  def write (time: Int): Int = {
    val now = write_acquire (time)
    write_release()
    now
  }

  def release_acquire (time: Int) {
    lock.lock()
    try {
      if (future < time)
        future = time
      state = makeState (future, false)
    } catch {
      case t: Throwable =>
        readers.signalAll()
        writers.signal()
        lock.unlock()
        throw t;
    }
  }

  def release_release() {
    readers.signalAll()
    writers.signal()
    lock.unlock()
  }

  def release (time: Int) {
    release_acquire (time)
    release_release()
  }}

object ConditionLock {

  case class Waiter (thread: Thread, time: Int, now: Int)

  def newSpace (implicit params: Params): LockSpace =
    LockSpace (params.nlocks) (new ConditionLock)
}