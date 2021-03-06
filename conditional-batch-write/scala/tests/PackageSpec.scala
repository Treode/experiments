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

import org.scalatest.FlatSpec
  
class PackageSpec extends FlatSpec {

  def foreachMasked (mask: Int, ks: Int*): Seq [Int] = {
    val b = Seq.newBuilder [Int]
    foreach (maskKeys (mask, ks)) (b += _)
    b.result
  }

  "foreachMasked" should "work" in {
    assert (foreachMasked (1) == Seq.empty)
    assert (foreachMasked (1, 0) == Seq (0))
    assert (foreachMasked (1, 1) == Seq (1))
    assert (foreachMasked (1, 0, 2) == Seq (0))
    assert (foreachMasked (3, 0, 2) == Seq (0, 2))
    assert (foreachMasked (3, 2, 0) == Seq (0, 2))
  }}
