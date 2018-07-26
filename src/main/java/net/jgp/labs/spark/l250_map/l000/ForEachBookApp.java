package net.jgp.labs.spark.l250_map.l000;

import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ForEachBookApp implements Serializable {
  private static final long serialVersionUID = -4250231621481140775L;

  private final class BookPrinter implements ForeachFunction<Row> {
    private static final long serialVersionUID = -3680381094052442862L;

    @Override
    public void call(Row r) throws Exception {
      System.out.println(r.getString(2) + " can be bought at " + r.getString(
          4));
    }
  }

  public static void main(String[] args) {
    ForEachBookApp app = new ForEachBookApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().appName("For Each Book").master(
        "local").getOrCreate();

    String filename = "data/books.csv";
    Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    df.show();

    df.foreach(new BookPrinter());
  }
}
