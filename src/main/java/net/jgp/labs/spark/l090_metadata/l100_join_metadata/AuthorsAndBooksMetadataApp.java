package net.jgp.labs.spark.l090_metadata.l100_join_metadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import net.jgp.labs.spark.x.utils.DataframeUtils;
import net.jgp.labs.spark.x.utils.FieldUtils;

public class AuthorsAndBooksMetadataApp {

  public static void main(String[] args) {
    AuthorsAndBooksMetadataApp app = new AuthorsAndBooksMetadataApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Authors and Books metadata")
        .master("local").getOrCreate();

    String filename = "data/authors.csv";
    Dataset<Row> authorsDf = spark.read()
        .format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load(filename);
    authorsDf = DataframeUtils.addMetadata(authorsDf, "x-source", filename);

    filename = "data/books.csv";
    Dataset<Row> booksDf = spark.read()
        .format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load(filename);
    booksDf = DataframeUtils.addMetadata(booksDf, "x-source", filename);

    Dataset<Row> libraryDf = authorsDf.join(booksDf, authorsDf.col("id")
        .equalTo(booksDf.col("authorId")), "full_outer");
    libraryDf.show();
    libraryDf.printSchema();

    System.out.println("Output of joining metadata");
    System.out.println("--------------------------");
    for (StructField field : libraryDf.schema().fields()) {
      System.out.println(FieldUtils.explain(field));
    }
  }
}
