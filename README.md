# Apache Spark Java Cookbook

## Environment
These labs rely on:
* Apache Spark 2.2.0 (base on Scala 2.11)
* Java 8

## Notes on Branches
The master branch will always contain the latest version of Spark, currently v2.2.0.

## Labs
A few labs around Apache Spark, exclusively in Java.

Organization is now in sub packages:

* l000_ingestion: Data ingestion from various sources.
* l020\_streaming: Data ingestion via streaming. Special note on [Streaming](file:src/main/java/net/jgp/labs/spark/l020_streaming/README.md).
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
