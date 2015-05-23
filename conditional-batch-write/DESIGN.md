# Conditional Batch Write

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

/** Scan the whole history up-to (on or before) time `t`. */
def scan (t: Int): Seq [Cell]
```

A client reads a row as of some time, and it can read values from the past&mdash;this is called multiversion concurrency control. Once a row has been read as-of some time, all later reads of that row as-of that time will get the same value. For example, suppose that a client reads the row `apple` as-of time `7`, and learns that it has the value `sour` from time `3`. All future clients which read the row `apple` as-of time `7` will find that it has the value `sour` from time `3`.

When a client writes a row, it must provide a time as a condition. The write will complete only if the latest time on that row is on or before the condition time, otherwise it will fail&mdash;this is known as optimistic concurrency control. Furthermore, the new value for the row will get a timestamp strictly after the time used by any client that read the row. Continuing the example, suppose a client writes the value `mushy` to the row `apple`. Since a client already read `apple` as-of time `7`, the write will get a timestamp after `7`.

Both read and write work in batches. Read supports batches simply for efficiency. Write supports batches with an additional guarantee: the write completes all-or-nothing. That is, if all rows in the write satisfy the condition time, then they will all be updated. If any row fails the condition time, then none of them will be updated. Thus the name conditional batch write. Conditional batch write can be used to implement transactions.

Why this rather than a basic hash table? First, conditional batch write provides atomic updates without requiring the client to hold locks. Second, it provides versioning which allows clients to [integrate caches][omvcc]. This makes conditional batch write a great tool to support remote clients. The server does not need to keep any state per client. The server does not need to clean up locks if the client crashes. The client and server do not need a sophisticated protocol to synchronize the client cache.

# Testing Function and Performance

This code explores different ways we might implement conditional batch write. For testing, we simulate transferring money between bank accounts. We use a table to associate accounts with balances. We pick two accounts at random, and transfer some random amount from one to the other. Since our focus is on performance rather than real banks, we allow the balances to be negative, as that simplifies coding.

```scala
val table = …                               // Make a new table
val rt = table.time                         // Get the current time
val (a1, a1) = …                            // Choose two account numbers
val (b1, b2) = read (rt, a1, a2)            // Read the balances as-of rt
val n = …                                   // Choose an amount to transfer
write (rt, Row (a1, b1-n), Row (a2, b2+n))  // Write the new balances
```

All accounts start with a balance of zero, so the initial total is zero. A transfer balances the debit and credit, so each transfer leaves the total at zero. To test the function of a table, we can run many transactions and then scan the entire history. At each point in time, the total of all balances should equal zero. To test the performance of a table, we can measure the time that elapsed while running many transactions.

# Using a Sorted Map

For `read (t, ks)`, we need to find a row by time and key. Suppose we have a map that sorts by its key and offers a floor operation.

```scala
/** Type `K` must be ordered. */
trait SortedMap [K, V] {

    case class Entry [K, V] (k: K, v: V)

    /** Read greatest element less than or equal to `k`.
      * Return null if there is no such element.
      */
    def floor (k: K): Entry
}
```

That map could be implemented by a red-black tree, AVL tree or skip list.The key for that map can combine the key and time for our table.

```scala
case class Key (k: Int, t: Int) extends Ordered [Key] {
    // …define comparator…
}
```

Then we can implement read and write for a single row. Our will behave as if all keys have value `0` at time `0`. Though this may not be appropriate behavior for a production implementation, it simplifies the code for our performance testing. 

```scala
var map = SortedMap [Key, Int]          // The internal map for our table.

var clock = 0                           // Latest time of past reads and writes.

def read (t: Int, k: Int): Value = {
    if (clock < t)                      // Raise latest time.
        clock = t
    val x = map.floor (Key (k, t))      // Find k on or before t.
    if (x == null || x.k.k != k)
        return Value (0, 0)             // All rows have 0 at time 0.
    else
        return Value (x.v, x.k.t)
}
        
def write (t: Int, k: Int, v: Int): Int = {
    val x = map.floor (Key (k, MaxInt))     // Look for the most recent value for k.
    if (x != null && x.k.t > t)             // Its write time must meet the condition.
        throw new StaleException
    clock += 1                              // Write time strictly after past reads.
    map.put (Key (k, clock), v)
}
```

We have shown how to read and write a single row. We leave it as an exercise for the reader to implement the batch versions.

Our performance tests try several implementations of sorted maps. These tests do not entail multithreading.

# Using a Hash Table of Sorted Maps

The implementation using a sorted map has runtime `O (lg N)` where N is the number of versioned rows. That is, if we have `K` keys and each with `V` versions, then `N = K*V`. We only need the `floor` operation to find the latest time for a given key, so we could use hash table of sorted maps. We would lookup the key in the hash table to get a sorted map of versions for that key. Then we would use the time to lookup the version on or before that time. Then the lookup time is `O(lg V)`. For large tables, `V` would be significantly less than `N`.

Our performance tests try several implementations of hash tables and sorted maps and combinations thereof. These tests do not entail multithreading.

# Handling Concurrency

We must ask: how can we exploit the multiple cores in our machine?

We might wrap the table operations with a mutex or a reader/writer lock. Our performance tests try both these options.

We might confine the table to a single thread that processes operations from a queue, and have client threads queue their operations. Our performance tests try a few implementations of thread-safe producer/consumer queues.

We might shard the table. We might guard the shards using a mutex, a reader/write lock, or thread confinement. Our performance tests try variations on these options.

[omvcc]: https://forum.treode.com/t/eventual-consistency-and-transactions-working-together/36 "Eventual Consistency and Transactions Working Together"
