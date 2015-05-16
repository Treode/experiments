package experiments

import scala.collection.{SortedMap, mutable}

/** Use Scala's mutable Map and immutable SortedMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class ScalaMutableMapOfSortedMap (hint: Int) extends Table {

  private val table =
    new mutable.HashMap [Int, SortedMap [Int, Int]]
      .withDefaultValue (SortedMap (Int.MaxValue -> 0))
  table.sizeHint (hint)

  private var clock = 0

  def time = clock

  private def read (x: Int, k: Int): Value = {
    val vs = table (k)
    val i = vs.iteratorFrom (x)
    if (!i.hasNext)
      return Value.empty
    val (x2, v) = i.next
    return Value (v, Int.MaxValue - x2)
  }

  def read (t: Int, ks: Int*): Seq [Value] = {
    val x = Int.MaxValue - t
    ks map (read (x, _))
  }

  private def prepare (r: Row): Int =
    table (r.k) .head._1

  private def prepare (t: Int, rs: Seq [Row]) {
    val max = Int.MaxValue - (rs.map (prepare (_)) .min)
    if (max > t) throw new StaleException (t, max)
  }

  private def commit (x: Int, r: Row) {
    (table get r.k) match {
      case Some (vs) => table.update (r.k, vs + (x -> r.v))
      case None => table.update (r.k, SortedMap (x -> r.v))
    }}

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

trait NewScalaMutableMapOfSortedMap extends NewTable {

  def parallel = false

  def newTable = new ScalaMutableMapOfSortedMap (naccounts)
}

