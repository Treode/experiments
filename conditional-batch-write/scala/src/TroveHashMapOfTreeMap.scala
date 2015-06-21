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

import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.procedure.TIntObjectProcedure
import java.util.TreeMap
import scala.collection.JavaConversions._

/** Use Trove's TIntObjectHashMap and Java's TreeMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class TroveHashMapOfTreeMap (implicit params: Params) extends Shard {

  private val table = new TIntObjectHashMap [TreeMap [Int, Int]] (params.naccounts)

  private def get (k: Int) = {
    var vs = table.get (k)
    if (vs == null) {
      vs = new TreeMap [Int, Int]
      table.put (k, vs)
    }
    vs
  }

  private def put (k: Int, v: Int, x: Int) = {
    var vs = table.get (k)
    if (vs == null) {
      vs = new java.util.TreeMap [Int, Int]
      table.put (k, vs)
    }
    vs.put (x, v)
  }

  def read (t: Int, k: Int): Value = {
    val x = Int.MaxValue - t
    val vs = get (k)
    val i = vs.tailMap (x)
    if (i.isEmpty)
      return Value.empty
    val (x2, v) = i.head
    return Value (v, Int.MaxValue - x2)
  }

  def prepare (k: Int): Int = {
    val vs = get (k)
    if (vs.isEmpty)
      return 0
    return Int.MaxValue - vs.firstKey
  }

  def commit (t: Int, k: Int, v: Int): Unit =
    put (k, v, Int.MaxValue - t)

  def scan (t: Int): Seq [Cell] = {
    val b = Seq.newBuilder [Cell]
    table.forEachEntry (new TIntObjectProcedure [TreeMap [Int, Int]] {
      def execute (k: Int, vs: TreeMap [Int, Int]): Boolean = {
        for {
          (x, v) <- vs
          t2 = Int.MaxValue - x
          if t2 <= t
        } b += Cell (k, v, t2)
        true
      }})
    b.result
  }

  def close() = ()
}

trait NewTroveHashMapOfTreeMap extends NewTable {

  def parallel = false

  def newTable (implicit params: Params): Table =
    new TableFromShard (new TroveHashMapOfTreeMap)
}
