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

import java.util.Arrays
import java.util.concurrent.{ExecutionException, Future}

package object experiments {

  case class Params (platform: String, nlocks: Int, nshards: Int, naccounts: Int, nbrokers: Int, ntransfers: Int) {

    override def toString =
      s"platform: $platform, " +
      s"nlocks: $nlocks, " +
      s"nshards: $nshards, " +
      s"naccounts: $naccounts, " +
      s"nbrokers: $nbrokers, " +
      s"ntransfers: $ntransfers"
  }

  implicit class RichFuture [A] (f: Future [A]) {

    def safeGet: A =
      try {
        f.get
      } catch {
        case t: ExecutionException =>
          throw t.getCause
      }}

  /** Get time from lock state. */
  def getTime (s: Int): Int =
    s >>> 1

  /** Get held from lock state. */
  def isHeld (s: Int): Boolean =
    (s & 1) == 1

  /** Pack lock state, from time `t` and held `h`, into a single int. */
  def makeState (t: Int, h: Boolean): Int =
    // Put held into lowest single bit, and time into remaining bits.
    if (h)
      (t << 1) | 1
    else
      (t << 1)

  /** Set held to `h` on lock state `s`. */
  def setHeld (s: Int, h: Boolean): Int =
    if (h)
      s | 1
    else
      s & -2

  /** Mask keys in `ks` and then sort. */
  def maskKeys (mask: Int, ks: Seq [Int]): Array [Int] = {
    val ns = new Array [Int] (ks.length)
    var i = 0
    while (i < ks.length) {
      ns (i) = ks (i) & mask
      i += 1
    }
    Arrays.sort (ns)
    ns
  }

  /** Mask keys in `rs` and then sort. */
  def maskRows (mask: Int, rs: Seq [Row]): Array [Int] = {
    val ns = new Array [Int] (rs.length)
    var i = 0
    while (i < rs.length) {
      ns (i) = rs (i) .k & mask
      i += 1
    }
    Arrays.sort (ns)
    ns
  }

  /** Repeat `f` foreach int skipping duplicates. */
  def foreach (ns: Seq [Int]) (f: Int => Any) {
    var i = 0
    var n = -1
    while (i < ns.length) {
      val m = ns (i)
      if (m > n) {
        f (m)
        n = m
      }
      i += 1
    }}}
