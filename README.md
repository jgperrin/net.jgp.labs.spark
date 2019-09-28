# Some Java examples for Apache Spark

Welcome to this project I started several years ago with this simple idea: let's use Spark with Java and not learn all those complex stuff like Hadoop or Scala. I am not that smart anyway...

This project is evolving in a book, creatively named "**[Spark in Action, 2nd edition, with Java](http://jgp.net/sia)**" published by Manning.

If you want to know more, and be guided through your Java and Spark learning process, I can only recommend to read the book at Manning: Spark with Java. Find out more about [Spark in Action, 2nd edition, on the Manning website](http://jgp.net/sia). The book contains more examples, more explanation, is professionally written and edited.

Here are the repos with the book examples:

[Chapter 1](https://github.com/jgperrin/net.jgp.books.spark.ch01) Introduction to Spark and deals with basic ingestion examples.

[Chapter 2](https://github.com/jgperrin/net.jgp.books.spark.ch02) Mental model around Spark.

[Chapter 3](https://github.com/jgperrin/net.jgp.books.spark.ch03) The majestic role of the dataframe.

[Chapter 4](https://github.com/jgperrin/net.jgp.books.spark.ch04) All about laziness.

[Chapter 5](https://github.com/jgperrin/net.jgp.books.spark.ch05) WIP

[Chapter 7](https://github.com/jgperrin/net.jgp.books.spark.ch07) Ingestion from files.

[Chapter 8](https://github.com/jgperrin/net.jgp.books.spark.ch08) Ingestion from databases.

[Chapter 9](https://github.com/jgperrin/net.jgp.books.spark.ch09) Ingestion from anything, right?

[Chapter 10](https://github.com/jgperrin/net.jgp.books.spark.ch10) WIP

[Chapter 11](https://github.com/jgperrin/net.jgp.books.spark.ch11) WIP

[Chapter 12](https://github.com/jgperrin/net.jgp.books.spark.ch12) WIP

[Chapter 13](https://github.com/jgperrin/net.jgp.books.spark.ch13) WIP

[Chapter 14](https://github.com/jgperrin/net.jgp.books.spark.ch14) WIP

[Chapter 15](https://github.com/jgperrin/net.jgp.books.spark.ch15) WIP

[Chapter 16](https://github.com/jgperrin/net.jgp.books.spark.ch16) WIP

[Chapter 17](https://github.com/jgperrin/net.jgp.books.spark.ch17) WIP



In the meanwhile, this project is still live, with more raw-level examples, that may (or may not) work.


## Environment
These labs rely on:
* Apache Spark 2.4.0 (based on Scala 2.11)
* Java 8

## Notes on Branches
The master branch will always contain the latest version of Spark, currently v2.4.0.

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


If you would like to see more labs, send your request to jgp at jgp dot net or [@jgperrin](https://twitter.com/jgperrin) on Twitter.
