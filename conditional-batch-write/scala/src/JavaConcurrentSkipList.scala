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

import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.JavaConversions._

/** Use a Java's ConcurrentSkipList to implement the map `(k, t) -> v`. The keys sort in reverse
  * chronological order, so searching for the ceiling of `(k, Int.MaxValue)` will find the most
  * recent value for the key. This is thread safe.
  */
class JavaConcurrentSkipListMap (lock: LockSpace) extends Table {

  private var table = new ConcurrentSkipListMap [Key, Int]

  def time = lock.time

  private def read (t: Int, k: Int): Value = {
    val e = table.ceilingEntry (Key (k, t))
    if (e == null)
      return Value.empty
    val Key (k2, t2) = e.getKey
    val v = e.getValue
    if (k2 != k)
      return Value.empty
    return Value (v, t2)
  }

  def read (t: Int, ks: Int*): Seq [Value] = {
    lock.read (t, ks)
    ks map (read (t, _))
  }

  private def prepare (r: Row): Int = {
    val e = table.ceilingEntry (Key (r.k, Int.MaxValue))
    if (e == null)
      return 0
    val Key (k2, t2) = e.getKey
    if (k2 != r.k)
      return 0
    return t2
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = rs.map (prepare (_)) .max
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (t: Int, r: Row): Unit =
    table.put (Key (r.k, t), r.v)

  private def commit (t: Int, rs: Seq [Row]): Unit =
    rs foreach (commit (t, _))

  def write (t: Int, rs: Row*): Int = {
    val wt = lock.write (lock.time, rs) + 1
    try {
      prepare (t, rs)
      commit (wt, rs)
      wt
    } finally {
      lock.release (wt, rs)
    }}

  def scan(): Seq [Cell] = {
    val now = lock.time
    lock.scan (now)
    for {
      (Key (k, t), v) <- table.toSeq
      if t < now
    } yield Cell (k, v, t)
  }

  def close() = ()
}

trait NewJavaConcurrentSkipListMap extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) = new JavaConcurrentSkipListMap (AqsLock.newSpace)
}
