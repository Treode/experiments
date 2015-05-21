package experiments

import java.util.{HashMap, TreeMap}
import scala.collection.JavaConversions._

/** Use Java's HashMap and TreeMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class JavaHashMapOfTreeMap (hint: Int = 16) extends Shard with Table {

  private val table = new HashMap [Int, TreeMap [Int, Int]] (hint)

  private var clock = 0

  def time = clock

  def read (t: Int, k: Int): Value = {
    val vs = table.get (k)
    if (vs == null)
      return Value.empty
    val i = vs.tailMap (Int.MaxValue - t)
    if (i.isEmpty)
      return Value.empty
    val (x2, v) = i.head
    return Value (v, Int.MaxValue - x2)
  }

  def read (t: Int, ks: Int*): Seq [Value] =
    ks map (read (t, _))

  def prepare (r: Row): Int = {
    val vs = table.get (r.k)
    if (vs == null)
      return 0
    return Int.MaxValue - vs.firstKey
  }

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = rs.map (prepare (_)) .max
    if (max > t) throw new StaleException (t, max)
  }

  def commit (t: Int, r: Row) {
    var vs = table.get (r.k)
    if (vs == null) {
      vs = new java.util.TreeMap [Int, Int]
      table.put (r.k, vs)
    }
    vs.put (Int.MaxValue - t, r.v)
  }

  private def commit (rs: Seq [Row]): Int = {
    clock += 1
    rs foreach (commit (clock, _))
    clock
  }

  def write (t: Int, rs: Row*): Int = {
    prepare (t, rs)
    commit (rs)
  }

  def scan (t: Int): Seq [Cell] =
    for {
      (k, vs) <- table.toSeq
      (x, v) <- vs
      t2 = Int.MaxValue - x
      if t2 < t
    } yield Cell (k, v, t2)

  def scan(): Seq [Cell] =
    for ((k, vs) <- table.toSeq; (x, v) <- vs) yield Cell (k, v, Int.MaxValue - x)

  def close() = ()
}

trait NewJavaHashMapOfTreeMap extends NewTable {

  def parallel = false

  def newTable = new JavaHashMapOfTreeMap (naccounts)
}
