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

import scala.collection.SortedMap

/** Use Scala's immutable Map and immutable SortedMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class ScalaMapOfSortedMap extends Table {

  private var table = Map.empty [Int, SortedMap [Int, Int]]
      .withDefaultValue (SortedMap (Int.MaxValue -> 0))

  private var clock = 0

  def time = clock

  private def read (x: Int, k: Int): Value = {
    val vs = table (k)
    val i = vs.iteratorFrom (x)
    if (i.hasNext) {
      val (x2, v) = i.next
      Value (v, Int.MaxValue - x2)
    } else {
      Value.empty
    }}

  def read (t: Int, ks: Int*): Seq [Value] = {
    val x = Int.MaxValue - t
    ks map (read (x, _))
  }

  private def prepare (r: Row): Int =
    table (r.k) .head._1

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = Int.MaxValue - (rs.map (prepare (_)) .min)
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (x: Int, r: Row) {
    (table get r.k) match {
      case Some (vs) => table += r.k -> (vs + (x -> r.v))
      case None => table += r.k -> SortedMap (x -> r.v)
    }}

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

  def scan(): Seq [Cell] =
    for ((k, vs) <- table.toSeq; (x, v) <- vs) yield Cell (k, v, Int.MaxValue - x)

  def close() = ()
}

trait NewScalaMapOfSortedMap extends NewTable {

  def parallel = false

  def newTable = new ScalaMapOfSortedMap
}
