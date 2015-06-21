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

case class PerfResult (name: String, platform: String, nshards: Int, nbrokers: Int, result: Double) {

  override def toString = f"$name, java, $platform, $nshards, $nbrokers, $result"
}

object PerfResult {

  def apply (name: String, result: Double) (implicit p: Params): PerfResult =
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
  * - `nhits` execute within `tolerance` of the running mean time,
  * - `ntrials` execute,
  * - `nseconds` pass,
  * whichever comes first.
  *
  * Report measurements that are within `tolerance` of the running mean time.
  */
trait TablePerf {

  val ntrials = 2000
  val nseconds = 60
  val nhits = 20
  val tolerance = 0.05
  val million = (1000 * 1000).toDouble

  def trial () (implicit params: Params): Long

  def perf () (implicit params: Params): PerfResult = {

    import params.{nbrokers, ntransfers}

    val name = getClass.getSimpleName
    println (s"$name, $params")

    val ops = ntransfers.toDouble
    val limit  = System.currentTimeMillis + nseconds * 1000
    var sum = 0.toDouble
    var hits = nhits

    for (trial <- 0 until ntrials) {
      val ns = this.trial().toDouble
      val x = ops / ns * million
      sum += x
      val n = (trial + 1).toDouble
      val mean = sum / n
      val dev = math.abs (x - mean) / mean
      if (dev <= tolerance) {
        println (f"$trial%5d: $x%8.2f ops/ms ($mean%8.2f)")
        hits -= 1
        if (hits == 0)
          return PerfResult (name, mean)
      }
      if (System.currentTimeMillis > limit)
        return PerfResult (name, mean)
    }
    val mean = sum / ntrials.toDouble
    PerfResult (name, mean)
  }}

class SyncTablePerf extends TablePerf with TableTools {
  this: NewTable =>

  def trial () (implicit params: Params): Long =
    withTable (transfers (_, parallel))
}

class AsyncTablePerf extends TablePerf with AsyncTableTools {
  this: NewAsyncTable =>

  def trial () (implicit params: Params): Long =
    withTable  { table => implicit scheduler =>
      transfers (table)
    }}

//
// Single-Threaded Strategies
//
// Cannot handle concurrent clients.
//

class JavaHashMapOfTreeMapPerf (implicit p: Params)
  extends SyncTablePerf with NewJavaHashMapOfTreeMap

class JavaTreeMapPerf (implicit p: Params)
  extends SyncTablePerf with NewJavaTreeMap

class ScalaMapOfSortedMapPerf (implicit p: Params)
  extends SyncTablePerf with NewScalaMapOfSortedMap

class ScalaMutableMapOfSortedMapPerf (implicit p: Params)
  extends SyncTablePerf with NewScalaMutableMapOfSortedMap

class ScalaSortedMapPerf (implicit p: Params)
  extends SyncTablePerf with NewScalaSortedMap

class TroveHashMapOfTreeMapPerf (implicit p: Params)
  extends SyncTablePerf with NewTroveHashMapOfTreeMap

//
// Single-Threaded Scheduler Strategies
//
// Queue tasks onto a single thread.
//

class SingleThreadExecutorPerf (implicit p: Params)
  extends SyncTablePerf with NewSingleThreadExecutorTable

class SimpleQueuePerf (implicit p: Params)
  extends SyncTablePerf with NewSimpleQueueTable

class ShardedQueuePerf (implicit p: Params)
  extends SyncTablePerf with NewShardedQueueTable

class JCToolsQueuePerf (implicit p: Params)
  extends SyncTablePerf with NewJCToolsQueueTable

//
// Thread-Safe Strategies, using AqsLock
//
// Handle concurrency some other way.
//

class JavaArrayListPerf (implicit p: Params)
  extends SyncTablePerf with NewJavaArrayList

class JavaConcurrentSkipListMapPerf (implicit p: Params)
  extends SyncTablePerf with NewJavaConcurrentSkipListMap

class SynchronizedTablePerf (implicit p: Params)
  extends SyncTablePerf  with NewSynchronizedTable

class SynchronizedShardedTablePerf (implicit p: Params)
  extends SyncTablePerf with NewSynchronizedShardedTable

class ReadWriteShardedTablePerf (implicit p: Params)
  extends SyncTablePerf with NewReadWriteShardedTable

class SingleThreadShardedTablePerf (implicit p: Params)
  extends SyncTablePerf with NewSingleThreadShardedTable

class FutureShardedTablePerf (implicit p: Params)
  extends SyncTablePerf with NewFutureShardedTable

class CollectorShardedTablePerf (implicit p: Params)
  extends SyncTablePerf with NewCollectorShardedTable

class DisruptorTablePerf (implicit p: Params)
  extends SyncTablePerf with NewDisruptorTable

//
// Alternative Logical-Lock strategies, using SynchronizedShardedTable
//

class ConditionLockPerf (implicit p: Params)
  extends SyncTablePerf with NewConditionLockTable

//
// Asynchronous strategies.
//

class FiberizedTablePerf (implicit p: Params)
  extends AsyncTablePerf with NewFiberizedTable

class FiberizedForkJoinTablePerf (implicit p: Params)
  extends AsyncTablePerf with NewFiberizedForkJoinTable

class FiberizedShardedTablePerf (implicit p: Params)
  extends AsyncTablePerf with NewFiberizedShardedTable

class FiberizedShardedForkJoinTablePerf (implicit p: Params)
  extends AsyncTablePerf with NewFiberizedShardedForkJoinTable

object Main {

  // Powers of 2, from 1 to availableProcessors (or next power of 2).
  val shards =
    Seq.tabulate (bitCount (highestOneBit (getRuntime.availableProcessors) - 1) + 1) (1 << _)

  val brokers = Seq (1, 2, 4, 8, 16, 32, 64)

  // Cannot handle concurrent clients.
  def unthreaded (results: PerfResults) (implicit params: Params) {
    results += (new JavaHashMapOfTreeMapPerf).perf()
    results += (new JavaTreeMapPerf).perf()
    results += (new ScalaMapOfSortedMapPerf).perf()
    results += (new ScalaMutableMapOfSortedMapPerf).perf()
    results += (new ScalaSortedMapPerf).perf()
    results += (new TroveHashMapOfTreeMapPerf).perf()
  }

  // Queue tasks onto a single thread.
  def queues (results: PerfResults) (implicit params: Params) {
    results += (new SingleThreadExecutorPerf).perf()
    results += (new SimpleQueuePerf).perf()
    results += (new ShardedQueuePerf).perf()
    results += (new JCToolsQueuePerf).perf()
  }

  // Handle concurrency some other way.
  def concurrent (results: PerfResults) (implicit params: Params) {
    results += (new JavaConcurrentSkipListMapPerf).perf()
    results += (new JavaArrayListPerf).perf() (params.copy (nshards = params.nlocks))
  }

  def concurrentSharded (results: PerfResults) (implicit params: Params) {
    results += (new SynchronizedTablePerf).perf()
    results += (new SynchronizedShardedTablePerf).perf()
    results += (new ReadWriteShardedTablePerf).perf()
    results += (new SingleThreadShardedTablePerf).perf()
    results += (new FutureShardedTablePerf).perf()
    results += (new CollectorShardedTablePerf).perf()
    results += (new DisruptorTablePerf).perf()
  }

  // Asynchronous strategies.
  def asynchronous (results: PerfResults) (implicit params: Params) {
    results += (new FiberizedTablePerf).perf()
    results += (new FiberizedForkJoinTablePerf).perf()
  }

  def asynchronousSharded (results: PerfResults) (implicit params: Params) {
    results += (new FiberizedShardedTablePerf).perf()
    results += (new FiberizedShardedForkJoinTablePerf).perf()
  }

  // Alternative strategies for the logical clock lock.
  def locks (results: PerfResults) (implicit params: Params) {
    results += (new ConditionLockPerf).perf()
  }

  def main (args: Array [String]) {

    val params = Params (
      platform = if (args.length > 0) args (0) else "unknown",
      nlocks = 128,
      nshards = 1,
      naccounts = 100,
      nbrokers = 1,
      ntransfers = 6400)

    val results = new PerfResults

    unthreaded (results) (params)

    for (nbrokers <- brokers)
      queues (results) (params.copy (nbrokers = nbrokers))

    for (nshards <- shards)
      for (nbrokers <- brokers)
        locks (results) (params.copy (nshards = nshards, nbrokers = nbrokers))

    for (nbrokers <- brokers) {
      concurrent (results) (params.copy (nbrokers = nbrokers))
      asynchronous (results) (params.copy (nbrokers = nbrokers))
    }

    for (nshards <- shards)
      for (nbrokers <- brokers) {
        concurrentSharded (results) (params.copy (nshards = nshards, nbrokers = nbrokers))
        asynchronousSharded (results) (params.copy (nshards = nshards, nbrokers = nbrokers))
      }

    println ("--")
    println (results)
  }}
