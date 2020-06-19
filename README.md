# Some Java examples for Apache Spark

Welcome to this project I started several years ago with this simple idea: let's use Spark with Java and not learn all those complex stuff like Hadoop or Scala. I am not that smart anyway...

This project is evolving in a book, creatively named "**[Spark in Action, 2nd edition, with Java](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp)**" published by Manning. If you want to know more, and be guided through your Java and Spark learning process, I can only recommend to read the book at Manning. Find out more about [Spark in Action, 2nd edition, on the Manning website](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). The book contains more examples, more explanation, is professionally written and edited. The book also talks about Spark with Python (PySpark) and Scala.

Here are the repos with the book examples:

[Chapter 1](https://github.com/jgperrin/net.jgp.books.spark.ch01) So, what is Spark, anyway? _An introduction to Spark with a simple ingestion example._

[Chapter 2](https://github.com/jgperrin/net.jgp.books.spark.ch02) Architecture and flows _Mental model around Spark and exporting data to PostgreSQL from Spark._

[Chapter 3](https://github.com/jgperrin/net.jgp.books.spark.ch03) The majestic role of the dataframe.

[Chapter 4](https://github.com/jgperrin/net.jgp.books.spark.ch04) Fundamentally lazy.

[Chapter 5](https://github.com/jgperrin/net.jgp.books.spark.ch05) Building a simple app for deployment _and_ Deploying your simple app.

[Chapter 7](https://github.com/jgperrin/net.jgp.books.spark.ch07) Ingestion from files.

[Chapter 8](https://github.com/jgperrin/net.jgp.books.spark.ch08) Ingestion from databases.

[Chapter 9](https://github.com/jgperrin/net.jgp.books.spark.ch09) Advanced ingestion: finding data sources & building your own.

[Chapter 10](https://github.com/jgperrin/net.jgp.books.spark.ch10) Ingestion through structured streaming.

[Chapter 11](https://github.com/jgperrin/net.jgp.books.spark.ch11) Working with Spark SQL.

[Chapter 12](https://github.com/jgperrin/net.jgp.books.spark.ch12) Transforming your data.

[Chapter 13](https://github.com/jgperrin/net.jgp.books.spark.ch13) Transforming entire documents.

[Chapter 14](https://github.com/jgperrin/net.jgp.books.spark.ch14) Extending transformations with user-defined functions (UDFs).

[Chapter 15](https://github.com/jgperrin/net.jgp.books.spark.ch15) Aggregating your data.

[Chapter 16](https://github.com/jgperrin/net.jgp.books.spark.ch16) Cache and checkpoint: enhancing Spark’s performances.

[Chapter 17](https://github.com/jgperrin/net.jgp.books.spark.ch17) Exporting data & building full data pipelines.


In the meanwhile, this project is still live, with more raw-level examples, that may (or may not) work.


## Environment
These labs rely on:
* Apache Spark v3.0.0 (based on Scala v2.12)
* Java 8

## Notes on Branches
The master branch will always contain the latest version of Spark, currently v3.0.0.

## Labs
A few labs around Apache Spark, exclusively in Java.

Organization is now in sub packages:

* l000_ingestion: Data ingestion from various sources.
* l020\_streaming: Data ingestion via streaming. Special note on [Streaming](src/main/java/net/jgp/labs/spark/l020_streaming/README.md).
* l050_connection: Connect to Spark.
* l100_checkpoint: Checkpoint introduced in Spark v2.1.0.
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

---

If you would like to see more labs, send your request to jgp at jgp dot net or [@jgperrin](https://twitter.com/jgperrin) on Twitter.
