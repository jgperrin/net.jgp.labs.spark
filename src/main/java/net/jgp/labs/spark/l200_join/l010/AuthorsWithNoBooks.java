package net.jgp.labs.spark.l200_join.l010;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AuthorsWithNoBooks {

  public static void main(String[] args) {
    AuthorsWithNoBooks app = new AuthorsWithNoBooks();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().appName("Authors and Books").master(
        "local").getOrCreate();

    String filename = "data/authors.csv";
    // @formatter:off
    Dataset<Row> authorsDf = spark.read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    // @formatter:on

    filename = "data/books.csv";
    // @formatter:off
    Dataset<Row> booksDf = spark.read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    // @formatter:on
    
    Dataset<Row> libraryDf = authorsDf.join(booksDf, authorsDf.col("id").equalTo(booksDf.col("authorId")), "left_anti");    
    libraryDf.show();
    libraryDf.printSchema();
  }
}
