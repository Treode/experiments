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

import java.util.{HashMap, TreeMap}
import scala.collection.JavaConversions._

/** Use Java's HashMap and TreeMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class JavaHashMapOfTreeMap (implicit params: Params) extends Shard {

  private val table = new HashMap [Int, TreeMap [Int, Int]] (params.naccounts)

  def read (t: Int, k: Int): Value = {
    val x = Int.MaxValue - t
    val vs = table.get (k)
    if (vs == null)
      return Value.empty
    val i = vs.tailMap (x)
    if (i.isEmpty)
      return Value.empty
    val (x2, v) = i.head
    return Value (v, Int.MaxValue - x2)
  }

  def prepare (k: Int): Int = {
    val vs = table.get (k)
    if (vs == null)
      return 0
    return Int.MaxValue - vs.firstKey
  }

  def commit (t: Int, k: Int, v: Int) {
    var vs = table.get (k)
    if (vs == null) {
      vs = new java.util.TreeMap [Int, Int]
      table.put (k, vs)
    }
    vs.put (Int.MaxValue - t, v)
  }

  def scan (t: Int): Seq [Cell] = {
    for {
      (k, vs) <- table.toSeq
      (x, v) <- vs
      t2 = Int.MaxValue - x
      if t2 <= t
    } yield Cell (k, v, t2)
  }

  def close() = ()
}

trait NewJavaHashMapOfTreeMap extends NewTable {

  def parallel = false

  def newTable (implicit params: Params): Table =
    new TableFromShard (new JavaHashMapOfTreeMap)
}
