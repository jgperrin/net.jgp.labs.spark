package net.jgp.labs.spark.l250_map.l100_pi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Computes Pi.
 * 
 * @author jgp
 */
public class PiComputeLambdaWithRddApp implements Serializable {
  private static final long serialVersionUID = -1547L;
  private static long counter = 0;

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    PiComputeLambdaWithRddApp app = new PiComputeLambdaWithRddApp();
    app.start(10);
  }

  /**
   * The processing code.
   */
  private void start(int slices) {
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaSparkPi")
        .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(integer -> {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      return (x * x + y * y <= 1) ? 1 : 0;
    }).reduce((integer, integer2) -> integer + integer2);

    System.out.println("Pi is roughly " + 4.0 * count / n);

    spark.stop();
  }
}
