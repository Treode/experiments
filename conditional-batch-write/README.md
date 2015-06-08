# Experiments for Conditional Batch Write

This code explores different ways we might implement conditional batch write.

It only examines in-memory approaches. It does not consider durably storing changes on disk, nor replicating changes to other machines. Later, we may check how those concerns affect the performance of different options. It investigates many approaches in [Scala][scala-lang]. We have started on some in C++ and [Rust][rust-lang].

You can read more about the conditional batch write and our test method in the [design document][design].

You can see our most recent [formal results][results] for the Scala tests. We ran these on quiet machines with no other undue workload. Undoubtedly, there are ways to improve those numbers. The informal C++ tests perform better, and without needing JIT warmup. Rust performs poorly; we’re probably using it wrong. We ran these tests off-hand on a desktop with open browsers, editors, and so on.

### Running the Scala Tests

You’ll need [SBT][sbt].

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write/scala
sbt assembly
java -jar target/scala-2.11/cbw.jar
```

### Running the C++ Tests

You’ll need `make` and `g++`. You’ll also need to install [thread building blocks][tbb]&mdash;be sure to [setup your environment][tbb-env].

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write/cpp
make runperf
```

### Running the Rust Tests

You’ll need the [nightly version][rust-nightly] of `cargo`.

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write/rust
cargo run --release
```

### Contributing

Feel free to use GitHub issues to ask questions or suggest improvements. Also, feel free to submit a pull request, however please sign the [contributor license agreement][cla-individual] first. If your employer has rights your intellectual property, your employer will need to sign the [Corporate CLA][cla-corporate].

[cla-corporate]: https://treode.github.io/store/cla-corporate.html

[cla-individual]: https://treode.github.io/store/cla-individual.html

[design]: DESIGN.md "Conditional Batch Write"

[results]: https://docs.google.com/spreadsheets/d/1_D93mvOwuUifNcDMpLt6JjXo0HkHWjmE7Si9aPby48E/edit?usp=sharing "Results"

[rust-lang]: http://www.rust-lang.org/ "The Rust Programming Language"

[rust-nightly]: http://doc.rust-lang.org/book/nightly-rust.html "Nightly Rust"

[scala-lang]: http://scala-lang.org/ "The Scala Programming Language"

[tbb]: https://www.threadingbuildingblocks.org/ "Intel® Threading Building Blocks"

[tbb-env]: https://software.intel.com/en-us/blogs/2007/09/09/globalized-tbb-environment-configuration-on-linux/ "Globalized TBB Environment Configuration on Linux"

[sbt]: http://www.scala-sbt.org/ "SBT"
