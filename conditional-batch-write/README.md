# Experiments for Conditional Batch Write

This code explores different ways we might implement conditional batch write.

It only examines in-memory approaches. It does not consider durably storing changes on disk, nor replicating changes to other machines. Later, we may check how those concerns affect the performance of different options. 

It investigates many approaches in Scala/Java. We have started on some in C++.

You can read more about the conditional batch write and our test method in the [design document][design].

You can [see our results][results] for the Scala/Java tests. Undoubtedly, there are ways to improve those numbers.

### Running the Scala Tests

You’ll need [SBT][sbt].

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write/scala
sbt assembly
java -jar target/scala-2.11/cbw.jar
```

### Running the C++ Tests

You’ll need `make` and `g++`.

```sh
git clone git@github.com:Treode/experiments.git
cd experiments/conditional-batch-write/cpp
make runperf
```

### Contributing

Feel free to use GitHub issues to ask questions or suggest improvements. Also, feel free to submit a pull request, however please sign the [contributor license agreement][cla-individual] first. If your employer has rights your intellectual property, your employer will need to sign the [Corporate CLA][cla-corporate].

[cla-corporate]: https://treode.github.io/store/cla-corporate.html

[cla-individual]: https://treode.github.io/store/cla-individual.html

[design]: DESIGN.md "Conditional Batch Write"

[results]: https://docs.google.com/spreadsheets/d/1_D93mvOwuUifNcDMpLt6JjXo0HkHWjmE7Si9aPby48E/edit?usp=sharing "Results"

[sbt]: http://www.scala-sbt.org/ "SBT"
