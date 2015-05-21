package experiments

/** Repeat transfers experiments until
  * - `count` execute within `tolerance` of the running mean time,
  * - ntrials execute,
  * whichever comes first.
  *
  * Report measurements that are within `tolerance` of the running mean time.
  */
trait TablePerf extends TableTools {
  this: NewTable =>

  val nlocks = 8
  val nshards = 8
  val naccounts = 100

  val ntrials = 2000
  val count = 20
  val tolerance = 0.05

  def perf() {

    println (getClass.getName)

    val M = 1000000.toDouble
    var sum = 0.toDouble
    var hits = count

    for (trial <- 0 until ntrials) {
      val ns = withTable (transfers (_, parallel)) .toDouble
      val ops = (nbrokers * ntransfers).toDouble
      val x = ops / ns * M
      sum += x
      val n = (trial + 1).toDouble
      val mean = sum / n
      val dev = math.abs (x - mean) / mean
      if (dev <= tolerance) {
        println (f"$trial%5d: $x%8.2f ops/ms ($mean%8.2f)")
        hits -= 1
        if (hits == 0)
          return
      }}}}

//
// Single-Threaded Strategies
//

class JavaHashMapOfTreeMapPerf extends TablePerf with NewJavaHashMapOfTreeMap

class JavaTreeMapPerf extends TablePerf with NewJavaTreeMap

class ScalaMapOfSortedMapPerf extends TablePerf with NewScalaMapOfSortedMap

class ScalaMutableMapOfSortedMapPerf extends TablePerf with NewScalaMutableMapOfSortedMap

class ScalaSortedMapPerf extends TablePerf with NewScalaSortedMap

class TroveHashMapOfTreeMapPerf extends TablePerf with NewTroveHashMapOfTreeMap

//
// Single-Threaded Scheduler Strategies, using JavaHashMapOfTreeMap
//

class SingleThreadExecutorPerf extends TablePerf with NewSingleThreadExecutorTable

class SimpleQueuePerf extends TablePerf with NewSimpleQueueTable

class ShardedQueuePerf extends TablePerf with NewShardedQueueTable

class JCToolsQueuePerf extends TablePerf with NewJCToolsQueueTable

//
// Thread-Safe Strategies
//

class DisruptorTablePerf extends TablePerf with NewDisruptorTable

class JavaConcurrentSkipListMapPerf extends TablePerf with NewJavaConcurrentSkipListMap

class SynchronizedTablePerf extends TablePerf with NewSynchronizedTable

class SynchronizedShardedTablePerf extends TablePerf with NewSynchronizedShardedTable

class ReadWriteShardedTablePerf extends TablePerf with NewReadWriteShardedTable

class SingleThreadShardedTablePerf extends TablePerf with NewSingleThreadShardedTable

object Main {

  def main (args: Array [String]) {

    // Measurements on 2.8 GHz Intel Core i7, Java 1.8.0_25

    //
    // Single-threaded measurements.
    //

    //(new JavaTreeMapPerf).perf()                          // 196 ops/ms
    //(new ScalaMapOfSortedMapPerf).perf()                  // 530 ops/ms
    //(new ScalaMutableMapOfSortedMapPerf).perf()           // 627 ops/ms
    //(new ScalaSortedMapPerf).perf()                       // 258 ops/ms

    // Fastest
    // Trove bloats code but not significantly faster.
    //(new TroveHashMapOfTreeMapPerf).perf()                // 767 ops/ms
    (new JavaHashMapOfTreeMapPerf).perf()                   // 730 ops/ms

    //
    // Single-threaded scheduler measurements.
    //

    //(new SingleThreadExecutorPerf).perf()                 //  93 ops/ms
    //(new SimpleQueuePerf).perf()                          //  90 ops/ms
    //(new ShardedQueuePerf).perf()                         //  91 ops/ms
    //(new JCToolsQueuePerf).perf()                         //  93 ops/ms

    //
    // Multithreaded measurements.
    //

    //(new DisruptorTablePerf).perf()                       //   5 ops/ms, 5!!!
    //(new JavaConcurrentSkipListMapPerf).perf()            // 208 ops/ms
    //(new SynchronizedTablePerf).perf()                    // 298 ops/ms
    //(new ReadWriteShardedTablePerf).perf()                // 182 ops/ms
    //(new SingleThreadShardedTablePerf).perf()             //  34 ops/ms

    // Fastest multithreaded approach.
    (new SynchronizedShardedTablePerf).perf()               // 531 ops/ms

  }}
