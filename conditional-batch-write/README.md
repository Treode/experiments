# Exploring implementations of Conditional Batch Write

Conditional batch write starts with the basic interface of a hash table, and extends it with timestamps.

```scala
/** Value `v` for key `k` as of time `t`. */
case class Cell (k: Int, time: Int, value: Int)

/** Value `v` for key `k`. */
case class Row (k: Int, v: Int)

/** Value `v` as of time `t`. */
case class Value (v: Int, t: Int)

/** Read values for the keys `ks` as-of (on or before) time `t`.
  * Return the values and the times that they were each written.
  * Arrange that subsequent writes on those rows use a time strictly after `t`.
  */
def read (t: Int, ks: Int*): Seq [Value]

/** Write rows `rs` if they have not changed since time `t`.
  * Return the time at which the new values were written.
  * Throw StaleException if one or more rows has changed since `t`.
  */
def write (t: Int, rs: Row*): Int

/** Scan the whole history up-to (on or before) time `t`.
  */
def scan (t: Int): Seq [Cell]
```

Conditional batch write provides a client atomic updates, without requiring the client to hold locks. This makes conditional batch write a great way to support remote clients, which could crash at any time. Should a client crash, there’s no need for a server to clean up locks.

This code explores different ways we might make an implementation concurrent. It examines in-memory approaches only. It does not consider durably storing changes on disk. It does not consider replicating changes to other machines. Later, we may check how those concerns affect the relative performance of different concurrent implementations.

This only looks at Scala/Java. Perhaps later we may try some C++ implementations.

You can [see our results][results]. Undoubtedly, there are ways to improve those numbers.

To build and run it yourself, you’ll need [SBT][sbt].

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write
sbt assembly
java -jar target/scala-2.11/cbw.jar
```

[sbt]: http://www.scala-sbt.org/

[results]: https://docs.google.com/spreadsheets/d/1_D93mvOwuUifNcDMpLt6JjXo0HkHWjmE7Si9aPby48E/edit?usp=sharing
