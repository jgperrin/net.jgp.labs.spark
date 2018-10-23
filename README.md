# Some Java examples for Apache Spark

Welcome to this project I started several years ago with this simple idea: let's use Spark with Java and not learn all those complex stuff like Hadoop or Scala. I am not that smart anyway...

This project is evolving in a book, creatively named "**[Spark in Action, 2nd edition, with Java](https://www.manning.com/books/spark-with-java)**" published by Manning.

If you want to know more, and be guided through your Java and Spark learning process, I can only recommend to read the book at Manning: Spark with Java. Find out more about [Spark in Action, 2nd edition, on the Manning website](https://www.manning.com/books/spark-with-java). The book contains more examples, more explanation, is professionally written and edited.

Here are the repos with the book examples:

[Chapter 1](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch01) Introduction to Spark and deals with basic ingestion examples.

[Chapter 2](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch02) Mental model around Spark.

[Chapter 3](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch03) The majestic role of the dataframe.

[Chapter 4](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch04) All about laziness.

[Chapter 5](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch05) WIP

[Chapter 6](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch06) WIP

[Chapter 7](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch07) Ingestion from files.

[Chapter 8](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch08) Ingestion from databases.

[Chapter 9](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch09) Ingestion from anything, right?

[Chapter 10](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch10) WIP

[Chapter 11](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch11) WIP

[Chapter 12](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch12) WIP

[Chapter 13](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch13) WIP

[Chapter 14](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch14) WIP

[Chapter 15](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch15) WIP

[Chapter 16](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch16) WIP

[Chapter 17](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch17) WIP

[Chapter 18](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch18) WIP

[Chapter 19](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch19) WIP

[Chapter 20](https://github.com/jgperrin/net.jgp.books.sparkWithJava.ch20) WIP


In the meanwhile, this project is still live, with more raw-level examples, that may (or may not) work.


## Environment
These labs rely on:
* Apache Spark 2.3.2 (based on Scala 2.11)
* Java 8

## Notes on Branches
The master branch will always contain the latest version of Spark, currently v2.3.2.

## Labs
A few labs around Apache Spark, exclusively in Java.

Organization is now in sub packages:

* l000_ingestion: Data ingestion from various sources.
* l020\_streaming: Data ingestion via streaming. Special note on [Streaming](src/main/java/net/jgp/labs/spark/l020_streaming/README.md).
* l050_connection: Connect to Spark.
* l100_checkpoint: Checkpoint introduced in v2.1.0.
* l150_udf: UDF (User Defined Functions).
* l200_join: added join examples.
* l250_map: map (in the context of mapping, not always linked to map/reduce).
* l300_reduce: reduce.
* l400\_industry\_formats: working with industry formats, limited, for now, to HL7 and FHIR.
* l500_misc: other examples.
* l600_ml: ML (Machine Learning).
* l700_save: saving your results.
* l800_concurrency: labs around concurrency access, work in progress.
* l900_analytics: More complex examples of using Spark for Analytics.


If you would like to see more labs, send your request to jgp@jgp.net or [@jgperrin](https://twitter.com/jgperrin) on Twitter.
