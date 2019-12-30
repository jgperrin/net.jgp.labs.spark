package net.jgp.labs.spark.l250_map.l102_pi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Computes Pi.
 * 
 * @author jgp
 */
public class PiComputeLambdaApp implements Serializable {
  private static final long serialVersionUID = -1547L;
  private static long counter = 0;

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    PiComputeLambdaApp app = new PiComputeLambdaApp();
    app.start(10);
  }

  /**
   * The processing code.
   */
  private void start(int slices) {
    int numberOfThrows = 100000 * slices;
    System.out.println("About to throw " + numberOfThrows
        + " darts, ready? Stay away from the target!");

    long t0 = System.currentTimeMillis();
    SparkSession spark = SparkSession
        .builder()
        .appName("Spark Pi with lambdas")
        .master("local[*]")
        .getOrCreate();

    long t1 = System.currentTimeMillis();
    System.out.println("Session initialized in " + (t1 - t0) + " ms");

    List<Integer> l = new ArrayList<>(numberOfThrows);
    for (int i = 0; i < numberOfThrows; i++) {
      l.add(i);
    }
    Dataset<Row> incrementalDf = spark
        .createDataset(l, Encoders.INT())
        .toDF();

    long t2 = System.currentTimeMillis();
    System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

    Dataset<Integer> dotsDs = incrementalDf
        .map((MapFunction<Row, Integer>) status -> {
          double x = Math.random() * 2 - 1;
          double y = Math.random() * 2 - 1;
          counter++;
          if (counter % 100000 == 0) {
            System.out.println("" + counter + " darts thrown so far");
          }
          return (x * x + y * y <= 1) ? 1 : 0;
        }, Encoders.INT());

    long t3 = System.currentTimeMillis();
    System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

    int dartsInCircle =
        dotsDs.reduce((ReduceFunction<Integer>) (x, y) -> x + y);
    long t4 = System.currentTimeMillis();
    System.out.println("Analyzing result in " + (t4 - t3) + " ms");

    System.out.println(
        "Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

    spark.stop();
  }
}
