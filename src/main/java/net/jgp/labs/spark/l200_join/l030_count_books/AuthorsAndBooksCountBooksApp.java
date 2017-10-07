package net.jgp.labs.spark.l200_join.l030_count_books;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AuthorsAndBooksCountBooksApp {

    public static void main(String[] args) {
        AuthorsAndBooksCountBooksApp app = new AuthorsAndBooksCountBooksApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Authors and Books")
                .master("local").getOrCreate();

        String filename = "data/authors.csv";
        Dataset<Row> authorsDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);
        authorsDf.show();

        filename = "data/books.csv";
        Dataset<Row> booksDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);
        booksDf.show();

        Dataset<Row> libraryDf = authorsDf
                .join(
                        booksDf,
                        authorsDf.col("id").equalTo(booksDf.col("authorId")),
                        "left")
                .withColumn("bookId", booksDf.col("id"))
                .drop(booksDf.col("id"))
                .groupBy(
                        authorsDf.col("id"),
                        authorsDf.col("name"),
                        authorsDf.col("link"))
                .count();

        libraryDf.show();
        libraryDf.printSchema();
    }
}
