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

import java.lang.Integer.{bitCount, highestOneBit}
import java.lang.Runtime.getRuntime
import scala.collection.mutable.Builder

case class PerfParams (platform: String, nshards: Int, nbrokers: Int) {

  override def toString = s"platform: $platform, nshards: $nshards, nbrokers: $nbrokers"
}

case class PerfResult (name: String, platform: String, nshards: Int, nbrokers: Int, result: Double) {

  override def toString = f"$name, java, $platform, $nshards, $nbrokers, $result"
}

object PerfResult {

  def apply (name: String, p: PerfParams, result: Double): PerfResult =
    new PerfResult (name, p.platform, p.nshards, p.nbrokers, result)
}

class PerfResults {

  private var results = List.empty [PerfResult]

  def += (result: PerfResult): Unit =
    results ::= result

  override def toString: String =
    "name, language, platform, nshards, nbrokers, ops/ms\n" + (results.reverse mkString "\n")
}

/** Repeat transfers experiments until
  * - `count` execute within `tolerance` of the running mean time,
  * - `ntrials` execute,
  * - `nseconds` pass,
  * whichever comes first.
  *
  * Report measurements that are within `tolerance` of the running mean time.
  */
class TablePerf (implicit p: PerfParams) extends TableTools {
  this: NewTable =>

  val nlocks = 1024
  val nshards = p.nshards
  val naccounts = 100

  val nbrokers = p.nbrokers
  val ntransfers = 1000

  val ntrials = 2000
  val nseconds = 60
  val count = 20
  val tolerance = 0.05
  val ops = (nbrokers * ntransfers).toDouble
  val million = (1000 * 1000).toDouble

  def perf(): PerfResult = {

    val name = getClass.getSimpleName
    println (s"$name, $p")

    val limit  = System.currentTimeMillis + nseconds * 1000
    var sum = 0.toDouble
    var hits = count

    for (trial <- 0 until ntrials) {
      val ns = withTable (transfers (_, parallel)) .toDouble
      val x = ops / ns * million
      sum += x
      val n = (trial + 1).toDouble
      val mean = sum / n
      val dev = math.abs (x - mean) / mean
      if (dev <= tolerance) {
        println (f"$trial%5d: $x%8.2f ops/ms ($mean%8.2f)")
        hits -= 1
        if (hits == 0)
          return PerfResult (name, p, mean)
      }
      if (System.currentTimeMillis > limit)
        return PerfResult (name, p, mean)
    }
    val mean = sum / ntrials.toDouble
    PerfResult (name, p, mean)
  }}

//
// Single-Threaded Strategies
//
// Cannot handle concurrent clients.
//

class JavaHashMapOfTreeMapPerf (implicit p: PerfParams)
  extends TablePerf with NewJavaHashMapOfTreeMap

class JavaTreeMapPerf (implicit p: PerfParams)
  extends TablePerf with NewJavaTreeMap

class ScalaMapOfSortedMapPerf (implicit p: PerfParams)
  extends TablePerf with NewScalaMapOfSortedMap

class ScalaMutableMapOfSortedMapPerf (implicit p: PerfParams)
  extends TablePerf with NewScalaMutableMapOfSortedMap

class ScalaSortedMapPerf (implicit p: PerfParams)
  extends TablePerf with NewScalaSortedMap

class TroveHashMapOfTreeMapPerf (implicit p: PerfParams)
  extends TablePerf with NewTroveHashMapOfTreeMap

//
// Single-Threaded Scheduler Strategies
//
//
// Queue tasks onto a single thread.
//

class SingleThreadExecutorPerf (implicit p: PerfParams)
  extends TablePerf with NewSingleThreadExecutorTable

class SimpleQueuePerf (implicit p: PerfParams)
  extends TablePerf with NewSimpleQueueTable

class ShardedQueuePerf (implicit p: PerfParams)
  extends TablePerf with NewShardedQueueTable

class JCToolsQueuePerf (implicit p: PerfParams)
  extends TablePerf with NewJCToolsQueueTable

//
// Thread-Safe Strategies
//
// Handle concurrency some other way.
//

class JavaConcurrentSkipListMapPerf (implicit p: PerfParams)
  extends TablePerf with NewJavaConcurrentSkipListMap

class SynchronizedTablePerf (implicit p: PerfParams)
  extends TablePerf  with NewSynchronizedTable

class SynchronizedShardedTablePerf (implicit p: PerfParams)
  extends TablePerf with NewSynchronizedShardedTable

class ReadWriteShardedTablePerf (implicit p: PerfParams)
  extends TablePerf with NewReadWriteShardedTable

class SingleThreadShardedTablePerf (implicit p: PerfParams)
  extends TablePerf with NewSingleThreadShardedTable

class FutureShardedTablePerf (implicit p: PerfParams)
  extends TablePerf with NewFutureShardedTable

class CollectorShardedTablePerf (implicit p: PerfParams)
  extends TablePerf with NewCollectorShardedTable

class DisruptorTablePerf (implicit p: PerfParams)
  extends TablePerf with NewDisruptorTable

object Main {

  // Powers of 2, from 1 to availableProcessors (or next power of 2).
  val shards =
    Seq.tabulate (bitCount (highestOneBit (getRuntime.availableProcessors) - 1) + 1) (1 << _)

  val brokers = Seq (2, 4, 8, 16, 32, 64)

  // Cannot handle concurrent clients.
  def unthreaded (results: PerfResults) (implicit params: PerfParams) {
    results += (new JavaHashMapOfTreeMapPerf).perf()
    results += (new JavaTreeMapPerf).perf()
    results += (new ScalaMapOfSortedMapPerf).perf()
    results += (new ScalaMutableMapOfSortedMapPerf).perf()
    results += (new ScalaSortedMapPerf).perf()
    results += (new TroveHashMapOfTreeMapPerf).perf()
  }

  // Queue tasks onto a single thread.
  def queues (results: PerfResults) (implicit params: PerfParams) {
    results += (new SingleThreadExecutorPerf).perf()
    results += (new SimpleQueuePerf).perf()
    results += (new ShardedQueuePerf).perf()
    results += (new JCToolsQueuePerf).perf()
  }

  // Handle concurrency some other way.
  def concurrent (results: PerfResults) (implicit params: PerfParams) {
    results += (new SynchronizedTablePerf).perf()
    results += (new JavaConcurrentSkipListMapPerf).perf()
    results += (new SynchronizedShardedTablePerf).perf()
    results += (new ReadWriteShardedTablePerf).perf()
    results += (new SingleThreadShardedTablePerf).perf()
    results += (new FutureShardedTablePerf).perf()
    results += (new CollectorShardedTablePerf).perf()
    results += (new DisruptorTablePerf).perf()
  }

  def main (args: Array [String]) {

    val platform =
      if (args.length > 0) args (0) else "unknown"

    val results = new PerfResults

    unthreaded (results) (PerfParams (platform, 1, 1))

    for (nbrokers <- brokers)
      queues (results) (PerfParams (platform, 1, nbrokers))

    for (nshards <- shards)
      for (nbrokers <- brokers)
        concurrent (results) (PerfParams (platform, nshards, nbrokers))

    println ("--")
    println (results)
  }}
