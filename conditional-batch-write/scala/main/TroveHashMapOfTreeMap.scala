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
class TroveHashMapOfTreeMap (hint: Int) extends Table {

  private val table = new TIntObjectHashMap [TreeMap [Int, Int]] (hint)

  private var clock = 0

  def time = clock

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

  private def read (x: Int, k: Int): Value = {
    val vs = get (k)
    val i = vs.tailMap (x)
    if (i.isEmpty)
      return Value.empty
    val (x2, v) = i.head
    return Value (v, Int.MaxValue - x2)
  }

  def read (t: Int, ks: Int*): Seq [Value] = {
    val x = Int.MaxValue - t
    ks map (read (x, _))
  }

  private def prepare (r: Row): Int = {
    val vs = get (r.k)
    if (vs.isEmpty)
      return Int.MaxValue
    return vs.firstKey
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = Int.MaxValue - (rs.map (prepare (_)) .min)
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (x: Int, r: Row): Unit =
    put (r.k, r.v, x)

  private def commit (rs: Seq [Row]): Int = {
    clock += 1
    val x = Int.MaxValue - clock
    rs foreach (commit (x, _))
    clock
  }

  def write (t: Int, rs: Row*): Int = {
    prepare (t, rs)
    commit (rs)
  }

  def scan(): Seq [Cell] = {
    val b = Seq.newBuilder [Cell]
    table.forEachEntry (new TIntObjectProcedure [TreeMap [Int, Int]] {
      def execute (k: Int, vs: TreeMap [Int, Int]): Boolean = {
        for ((x, v) <- vs)
          b += Cell (k, v, Int.MaxValue - x)
        true
      }})
    b.result
  }

  def close() = ()
}

trait NewTroveHashMapOfTreeMap extends NewTable {

  def parallel = false

  def newTable = new TroveHashMapOfTreeMap (naccounts)
}
