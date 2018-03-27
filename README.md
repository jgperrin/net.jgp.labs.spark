# Some Java examples for Apache Spark

If you want to know more, and be guided through your Java and Spark process, I can only recommend my book at Manning: Spark with Java. Find out more about [Spark with Java on the Manning website](https://www.manning.com/books/spark-with-java). The book contains more examples, more explanation, is professionally written and edited.

## Environment
These labs rely on:
* Apache Spark 2.3.0 (based on Scala 2.11)
* Java 8

## Notes on Branches
The master branch will always contain the latest version of Spark, currently v2.3.0.

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


If you would like to see more labs, send your request to jgp@jgp.net or @jgperrin on Twitter.
