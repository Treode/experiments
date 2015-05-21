package experiments

class DisruptorTablePerf extends TablePerf with NewDisruptorTable

class JavaConcurrentSkipListMapPerf extends TablePerf with NewJavaConcurrentSkipListMap

class JavaHashMapOfTreeMapPerf extends TablePerf with NewJavaHashMapOfTreeMap

class SynchronizedJavaHashMapOfTreeMapPerf extends TablePerf with NewSynchronizedJavaHashMapOfTreeMap

class SingleThreadExecutorJavaHashMapOfTreeMapPerf extends TablePerf with NewSingleThreadExecutorJavaHashMapOfTreeMap

class SynchronizedShardedTablePerf extends TablePerf with NewSynchronizedShardedTable

class ReadWriteShardedTablePerf extends TablePerf with NewReadWriteShardedTable

class SingleThreadShardedTablePerf extends TablePerf with NewSingleThreadShardedTable

class SimpleQueueJavaHashMapOfTreeMapPerf extends TablePerf with NewSimpleQueueJavaHashMapOfTreeMap

class JavaTreeMapPerf extends TablePerf with NewJavaTreeMap

class ScalaMapOfSortedMapPerf extends TablePerf with NewScalaMapOfSortedMap

class ScalaMutableMapOfSortedMapPerf extends TablePerf with NewScalaMutableMapOfSortedMap

class ScalaSortedMapPerf extends TablePerf with NewScalaSortedMap

class TroveHashMapOfTreeMapPerf extends TablePerf with NewTroveHashMapOfTreeMap

object Main {

  def main (args: Array [String]) {

    // Measurements 2.8 GHz Intel Core i7, Java 1.8.0_25

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
    // Multithreaded measurements.
    //

    //(new DisruptorTablePerf).perf()                       //   5 ops/ms, 5!!!
    //(new JavaConcurrentSkipListMapPerf).perf()            // 208 ops/ms
    //(new SimpleQueueJavaHashMapOfTreeMapPerf).perf()      //  90 ops/ms
    //(new SingleThreadExecutorJavaHashMapOfTreeMapPerf).perf() //  93 ops/ms
    //(new SynchronizedJavaHashMapOfTreeMapPerf).perf()     // 298 ops/ms
    //(new ReadWriteShardedTablePerf).perf()                // 182 ops/ms

    // Fastest multithreaded approach.
    // SingleThreadExecutorJavaHashMapOfTreeMapPerf cause concern for SingleThreadShardedTablePerf.
    //(new SingleThreadShardedTablePerf).perf()             // 521 ops/ms
    (new SynchronizedShardedTablePerf).perf()               // 531 ops/ms

  }}
