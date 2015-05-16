package experiments

import scala.collection.SortedMap

/** Use Scala's immutable Map and immutable SortedMap to implement the map `k -> t' -> v`. We set
  * `t' = Int.MaxValue - x`, so searching for the ceiling of `Int.MaxValue` will find the the most
  * recent value for the key. This is not thread safe.
  */
class ScalaMapOfSortedMap extends Table {

  private var table = Map.empty [Int, SortedMap [Int, Int]]
      .withDefaultValue (SortedMap (Int.MaxValue -> 0))

  private var clock = 0

  def time = clock

  private def read (x: Int, k: Int): Value = {
    val vs = table (k)
    val i = vs.iteratorFrom (x)
    if (i.hasNext) {
      val (x2, v) = i.next
      Value (v, Int.MaxValue - x2)
    } else {
      Value.empty
    }}

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
      case Some (vs) => table += r.k -> (vs + (x -> r.v))
      case None => table += r.k -> SortedMap (x -> r.v)
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

trait NewScalaMapOfSortedMap extends NewTable {

  def parallel = false

  def newTable = new ScalaMapOfSortedMap
}
