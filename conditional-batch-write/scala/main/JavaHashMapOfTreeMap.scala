package experiments

import java.util.{HashMap, TreeMap}
import scala.collection.JavaConversions._

/** Use Java's HashMap and TreeMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class JavaHashMapOfTreeMap (hint: Int) extends Table {

  private val table = new HashMap [Int, TreeMap [Int, Int]] (hint)

  private var clock = 0

  def time = clock

  private def get (k: Int) = {
    var vs = table.get (k)
    if (vs == null) {
      vs = new TreeMap [Int, Int]
      table.put (k, vs)
    }
    vs
  }

  private def put (k: Int, v: Int, x: Int) = {
    var vs = table.get (k)
    if (vs == null) {
      vs = new java.util.TreeMap [Int, Int]
      table.put (k, vs)
    }
    vs.put (x, v)
  }

  private def read (x: Int, k: Int): Value = {
    val vs = get (k)
    val i = vs.tailMap (x)
    if (i.isEmpty)
      return Value.empty
    val (x2, v) = i.head
    return Value (v, Int.MaxValue - x2)
  }

  def read (t: Int, ks: Int*): Seq [Value] = {
    val x = Int.MaxValue - t
    ks map (read (x, _))
  }

  private def prepare (r: Row): Int = {
    val vs = get (r.k)
    if (vs.isEmpty)
      return Int.MaxValue
    return vs.firstKey
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = Int.MaxValue - (rs.map (prepare (_)) .min)
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (x: Int, r: Row): Unit =
    put (r.k, r.v, x)

  private def commit (rs: Seq [Row]): Int = {
    clock += 1
    val x = Int.MaxValue - clock
    rs foreach (commit (x, _))
    clock
  }

  def write (t: Int, rs: Row*): Int = {
    prepare (t, rs)
    commit (rs)
  }

  def scan(): Seq [Cell] =
    for ((k, vs) <- table.toSeq; (x, v) <- vs) yield Cell (k, v, Int.MaxValue - x)

  def close() = ()
}

trait NewJavaHashMapOfTreeMap extends NewTable {

  def parallel = false

  def newTable = new JavaHashMapOfTreeMap (naccounts)
}

trait NewSynchronizedJavaHashMapOfTreeMap extends NewTable {

  def parallel = true

  def newTable = new SynchronizedTable (new JavaHashMapOfTreeMap (naccounts))
}

trait NewSingleThreadJavaHashMapOfTreeMap extends NewTable {

  def parallel = true

  def newTable = new SingleThreadTable (new JavaHashMapOfTreeMap (naccounts))
}

trait NewQueuedJavaHashMapOfTreeMap extends NewTable {

  def parallel = true

  def newTable = new QueuedTable (new JavaHashMapOfTreeMap (naccounts))
}
