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

  def read (time: Int) {
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
    } finally {
      lock.unlock()
    }
  }

  def write (time: Int): Int = {
    lock.lock()
    try {
      while (isHeld (state))
        writers.await()
      var now = getTime (state)
      if (now < time)
        now = time
      state = makeState (now, true)
      now
    } finally {
      lock.unlock()
    }
  }

  def release (time: Int) {
    lock.lock()
    try {
      if (future < time)
        future = time
      state = makeState (future, false)
      readers.signalAll()
      writers.signal()
    } finally {
      lock.unlock()
    }
  }
}

object ConditionLock {

  case class Waiter (thread: Thread, time: Int, now: Int)

  def space (nlocks: Int): LockSpace =
    LockSpace (nlocks) (new ConditionLock)
}