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

import java.util.concurrent.atomic.AtomicReference

import CasList.Node

/** Use CAS to push onto a linked list. Pushes new values onto the head of the list, so each new
  * value is earlier in the traversal; the logical clock ensures each new value has a greater
  * timestamp. This is intended to be used in a sharded table.
  */
class CasList (implicit params: Params) extends Shard {

  private val nodes = new AtomicReference [Node] (null)

  def read (t: Int, k: Int): Value = {
    var n = nodes.get
    while (n != null) {
      if (n.k == k && n.t <= t)
        return Value (n.v, n.t)
      n = n.next
    }
    return Value.empty
  }

  def prepare (k: Int): Int = {
    var n = nodes.get
    while (n != null) {
      if (n.k == k)
        return n.t
      n = n.next
    }
    return 0
  }

  def commit (t: Int, k: Int, v: Int) {
    val n = new Node (k, v, t, nodes.get)
    while (!nodes.compareAndSet (n.next, n))
      n.next = nodes.get
  }

  def scan (t: Int): Seq [Cell] = {
    val b = Seq.newBuilder [Cell]
    var n = nodes.get()
    while (n != null) {
      b += Cell (n.k, n.v, n.t)
      n = n.next
    }
    b.result
  }

  def close() = ()
}

object CasList {

  class Node (val k: Int, val v: Int, val t: Int, var next: Node)
}

trait NewCasList extends NewTable {

  def parallel = true

  def newTable (implicit params: Params): Table =
    ShardedTable (AqsLock.newSpace) (new CasList)
}
