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

import java.util.ArrayList
import scala.collection.JavaConversions._

/** Use Java's ArrayList to store cells. Pushes new values onto the back of the list, so each new
  * value has a greater index; the logical clock ensures each new value has a greater timestamp.
  * This is intended to be used in a sharded table. This is not thread safe.
  */
class JavaArrayList (implicit params: Params) extends Shard {

  private val cells = new ArrayList [Cell]

  def read (t: Int, k: Int): Value = {
    var i = cells.size - 1
    while (i >= 0) {
      val c = cells(i)
      if (c.k == k && c.t <= t)
        return Value (c.v, c.t)
      i -= 1
    }
    return Value.empty
  }

  def prepare (k: Int): Int = {
    var i = cells.size - 1
    while (i >= 0) {
      val c = cells(i)
      if (c.k == k)
        return c.t
      i -= 1
    }
    return 0
  }

  def commit (t: Int, k: Int, v: Int) {
    cells.add (Cell (k, v, t))
  }

  def scan (t: Int): Seq [Cell] = {
    for {
      c <- cells
      if c.t <= t
    } yield c
  }

  def close() = ()
}

trait NewJavaArrayList extends NewTable {

  def parallel = true

  def newTable (implicit params: Params): Table =
    ShardedTable (AqsLock.newSpace) (new SynchronizedShard (new JavaArrayList))
}
