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

import java.util.TreeMap
import scala.collection.JavaConversions._

/** Use Java's TreeMap to implement the map `(k, t) -> v`. The keys sort in reverse chronological
  * order, so searching for `(k, Int.MaxValue)` will find the most recent value for the key. This
  * is not thread safe.
  */
class JavaTreeMap extends Table {

  private var table = new TreeMap [Key, Int]

  private var clock = 0

  def time = clock

  private def read (t: Int, k: Int): Value = {
    val i = table.tailMap (Key (k, t))
    if (i.isEmpty)
      return Value.empty
    val (Key (k2, t2), v) = i.head
    if (k2 != k)
      return Value.empty
    return Value (v, t2)
  }

  def read (t: Int, ks: Int*): Seq [Value] =
    ks map (read (t, _))

  private def prepare (r: Row): Int = {
    val i = table.tailMap (Key (r.k, Int.MaxValue))
    if (i.isEmpty)
      return 0
    val (Key (k, t), _) = i.head
    if (k != r.k)
      return 0
    return t
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = rs.map (prepare (_)) .max
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (t: Int, r: Row): Unit =
    table += Key (r.k, t) -> r.v

  private def commit (rs: Seq [Row]): Int = {
    clock += 1
    rs foreach (commit (clock, _))
    clock
  }

  def write (t: Int, rs: Row*): Int = {
    prepare (t, rs)
    commit (rs)
  }

  def scan(): Seq [Cell] =
    for ((Key (k, t), v) <- table.toSeq) yield Cell (k, v, t)

  def close() = ()
}

trait NewJavaTreeMap extends NewTable {

  def parallel = false

  def newTable = new JavaTreeMap
}
