package experiments

import org.scalatest.FlatSpec

/** Functional tests for the implementations. */
trait TableBehaviors extends FlatSpec with TableTools {
  this: NewTable =>

  val nlocks = 8
  val nshards = 8
  val naccounts = 100

  def assertSeq [A] (expected: A*) (actual: Seq [A]): Unit =
    assertResult (expected) (actual)

  /** The physics and chemstry notion of "conserved". */
  def assertMoneyConserved (table: Table) {
    val history = table.scan()
      .groupBy (_.t)
      .toSeq
      .sortBy (_._1)
    var tracker = Map.empty [Int, Int]
    for ((t, cs) <- history) {
      for (c <- cs)
        tracker += c.k -> c.v
      val sum = tracker.values.sum
      assert (sum == 0)
    }}

  s"A ${getClass.getName}" should "read 0 for any key" in {
    withTable { table =>
      assertSeq (Value.empty) (table.read (0, 0))
      assertSeq (Value.empty) (table.read (1, 0))
    }}

  it should "read what was put" in {
    withTable { table =>
      table.write (0, Row (0, 1))
      assertSeq (Value (1, 1)) (table.read (1, 0))
    }}

  it should "read and write batches" in {
    withTable { table =>
      table.write (0, Row (0, 1), Row (1, 2))
      assertSeq (Value (1, 1), Value (2, 1)) (table.read (1, 0, 1))
    }}

  it should "reject a stale write" in {
    withTable { table =>
      table.write (0, Row (0, 1))
      intercept [StaleException] (table.write (0, Row (0, 2)))
      assertSeq (Value (1, 1)) (table.read (1, 0))
    }}

  it should "preserve the money supply running serially" in {
    withTable { table =>
      transfers (table, false)
      assertMoneyConserved (table)
    }}

  if (parallel) {
    it should "preserve the money supply running in parallel" in {
      withTable { table =>
        transfers (table, true)
        assertMoneyConserved (table)
      }}}}

class DisruptorTableSpec extends TableBehaviors with NewDisruptorTable

class JavaConcurrentSkipListMapSpec extends TableBehaviors with NewJavaConcurrentSkipListMap

class JavaHashMapOfTreeMapSpec extends TableBehaviors with NewJavaHashMapOfTreeMap

class SynchronizedJavaHashMapOfTreeMapSpec extends TableBehaviors with NewSynchronizedJavaHashMapOfTreeMap

class SingleThreadJavaHashMapOfTreeMapSpec extends TableBehaviors with NewSingleThreadExecutorJavaHashMapOfTreeMap

class QueuedJavaHashMapOfTreeMapSpec extends TableBehaviors with NewSimpleQueueJavaHashMapOfTreeMap

class JavaTreeMapSpec extends TableBehaviors with NewJavaTreeMap

class ScalaMapOfSortedMapSpec extends TableBehaviors with NewScalaMapOfSortedMap

class ScalaMutableMapOfSortedMapSpec extends TableBehaviors with NewScalaMutableMapOfSortedMap

class ScalaSortedMapSpec extends TableBehaviors with NewScalaSortedMap

class SingleThreadShardedTableSpec extends TableBehaviors with NewSynchronizedShardedTable

class ReadWriteShardedTableSpec extends TableBehaviors with NewReadWriteShardedTable

class SynchronizedShardedTableSpec extends TableBehaviors with NewSynchronizedShardedTable

class TroveHashMapOfTreeMapSpec extends TableBehaviors with NewTroveHashMapOfTreeMap
