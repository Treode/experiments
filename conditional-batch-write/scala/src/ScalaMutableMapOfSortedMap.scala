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

import scala.collection.{SortedMap, mutable}

/** Use Scala's mutable Map and immutable SortedMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class ScalaMutableMapOfSortedMap (implicit params: Params) extends Shard {

  private val table =
    new mutable.HashMap [Int, SortedMap [Int, Int]]
      .withDefaultValue (SortedMap (Int.MaxValue -> 0))
  table.sizeHint (params.naccounts)

  def read (t: Int, k: Int): Value = {
    val x = Int.MaxValue - t
    val vs = table (k)
    val i = vs.iteratorFrom (x)
    if (!i.hasNext)
      return Value.empty
    val (x2, v) = i.next
    return Value (v, Int.MaxValue - x2)
  }

  def prepare (k: Int): Int =
    Int.MaxValue - table (k) .head._1

  def commit (t: Int, k: Int, v: Int) {
    val x = Int.MaxValue - t
    (table get k) match {
      case Some (vs) => table.update (k, vs + (x -> v))
      case None => table.update (k, SortedMap (x -> v))
    }}

  def scan (t: Int): Seq [Cell] =
    for {
      (k, vs) <- table.toSeq
      (x, v) <- vs
      t2 = Int.MaxValue - x
      if t2 <= t
    } yield Cell (k, v, t2)

  def close() = ()
}

trait NewScalaMutableMapOfSortedMap extends NewTable {

  def parallel = false

  def newTable (implicit params: Params): Table =
    new TableFromShard (new ScalaMutableMapOfSortedMap)
}

