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

class JavaArray extends LockingShard {

  val lock = new ConditionLock
  val cells = new Array [Cell] (1000)
  var head = cells.size

  def time: Int =
    lock.time

  private def _read (head: Int, t: Int, k: Int): Value = {
    var i = head
    while (i < cells.size) {
      val c = cells (i)
      if (c.k == k && c.t <= t)
        return Value (c.v, c.t)
      i += 1
    }
    Value(0, 0)
  }

  def read (t: Int, k: Int): Value = {
    lock.read_acquire (t)
    val h = head
    lock.read_release()
    _read (h, t, k)
  }

  private def _prepare (head: Int, k: Int): Int = {
    var i = head
    while (i < cells.size) {
      val c = cells (i)
      if (c.k == k)
        return c.t
      i += 1
    }
    0
  }

  def prepare (ct: Int, k: Int): (Int, Int) = {
    val lt = lock.write_acquire (ct)
    val h = head;
    lock.write_release()
    val vt = _prepare (h, k);
    (vt, lt)
  }

  def prepare (ct: Int, k1: Int, k2: Int): (Int, Int) = {
    val lt = lock.write_acquire (ct)
    val h = head
    lock.write_release()
    val vt1 = _prepare(h, k1)
    val vt2 = _prepare(h, k2)
    (math.max (vt1, vt2), lt)
  }

  def commit (t: Int, k: Int, v: Int) {
    lock.release_acquire (t)
    assert (head > 0)
    cells (head-1) = new Cell (k, v, t)
    head -= 1
    lock.release_release()
  }

  def commit (t: Int, k1: Int, v1: Int, k2: Int, v2: Int) {
    lock.release_acquire (t)
    assert (head > 1)
    cells (head-1) = new Cell (k1, v1, t)
    cells (head-2) = new Cell (k2, v2, t)
    head -= 2
    lock.release_release()
  }

  def abort() {
    lock.release (0)
  }

  def scan (t: Int): Seq [Cell] = {
    lock.read_acquire (t)
    var i = head
    lock.read_release()
    val b = Seq.newBuilder [Cell]
    while (i < cells.size) {
      val c = cells (i)
      if (c.t <= t)
        b += c
      i += 1;
    }
    b.result
  }}

trait NewJavaArray extends NewTable {

  def parallel = true

  def newTable (implicit params: Params) = LockingShardTable (new JavaArray)
}