package net.jgp.labs.spark.l090_metadata.l000_add_metadata;

import static org.apache.spark.sql.functions.col;

import java.util.Date;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

import net.jgp.labs.spark.x.utils.ColumnUtils;
import net.jgp.labs.spark.x.utils.DataframeUtils;
import net.jgp.labs.spark.x.utils.FieldUtils;

public class MetadataApp {

  public static void main(String[] args) {
    MetadataApp app = new MetadataApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Modifying metadata")
        .master("local")
        .getOrCreate();

    String format = "csv";
    String filename = "data/books.csv";
    Dataset<Row> df = spark.read().format(format)
        .option("inferSchema", true)
        .option("header", true)
        .load(filename);
    df.show();
    df.printSchema();

    System.out.println("Pass #0");
    System.out.println("-------");
    for (StructField field : df.schema().fields()) {
      System.out.println(FieldUtils.explain(field));
    }

    long i = 0;
    for (String colName : df.columns()) {
      System.out.println("Column: " + colName);
      Column col = col(colName);

      Metadata metadata = new MetadataBuilder()
          .putString("x-source", filename)
          .putString("x-format", format)
          .putLong("x-order", i++)
          .build();

      System.out.println("Column: " + col);
      df = df.withColumn(colName, col, metadata);
    }

    df.printSchema();
    System.out.println("Pass #1");
    System.out.println("-------");
    for (StructField field : df.schema().fields()) {
      System.out.println(FieldUtils.explain(field));
    }

    for (String colName : df.columns()) {
      System.out.println("Column: " + colName);
      Column col = col(colName);

      Metadata metadata = new MetadataBuilder()
          .withMetadata(ColumnUtils.getMetadata(df, colName))
          .putString("x-process-date", new Date().toString())
          .build();

      System.out.println("Column: " + col);
      df = df.withColumn(colName, col, metadata);
    }

    df.printSchema();
    System.out.println("Pass #2");
    System.out.println("-------");
    for (StructField field : df.schema().fields()) {
      System.out.println(FieldUtils.explain(field));
    }

    for (String colName : df.columns()) {
      df = DataframeUtils.addMetadata(df, colName, "x-user", "jgp");
    }

    df.printSchema();
    System.out.println("Pass #3");
    System.out.println("-------");
    for (StructField field : df.schema().fields()) {
      System.out.println(FieldUtils.explain(field));
    }
  }
}
