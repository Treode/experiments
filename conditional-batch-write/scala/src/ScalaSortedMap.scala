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

/** Use Scala's immutable SortedMap to implement the map `(k, t) -> v`. The keys sort in reverse
  * chronological order, so searching for `(k, Int.MaxValue)` will find the most recent value for
  * the key. This is not thread safe.
  */
class ScalaSortedMap extends Shard {

  private var table = SortedMap.empty [Key, Int]

  def read (t: Int, k: Int): Value = {
    val i = table.iteratorFrom (Key (k, t))
    if (!i.hasNext)
      return Value.empty
    val (Key (k2, t2), v) = i.next
    if (k2 != k)
      return Value.empty
    return Value (v, t2)
  }

  def prepare (k: Int): Int = {
    val i = table.iteratorFrom (Key (k, Int.MaxValue))
    if (!i.hasNext)
      return 0
    val (Key (k2, t2), _) = i.next
    if (k2 != k)
      return 0
    return t2
  }

  def commit (t: Int, k: Int, v: Int): Unit =
    table += Key (k, t) -> v

  def scan (t: Int): Seq [Cell] =
    for {
      (Key (k, t2), v) <- table.toSeq
      if t2 <= t
    } yield Cell (k, v, t2)

  def close() = ()
}

trait NewScalaSortedMap extends NewTable {

  def parallel = false

  def newTable (implicit params: Params): Table =
    new TableFromShard (new ScalaSortedMap)
}
