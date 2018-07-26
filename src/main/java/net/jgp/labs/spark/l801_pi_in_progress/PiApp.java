package net.jgp.labs.spark.l801_pi_in_progress;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.x.datasource.SubStringCounterDataSource;

public class PiApp {
  private static transient Logger log = LoggerFactory.getLogger(
      SubStringCounterDataSource.class);
  private static final int NUM_SAMPLES = 10;

  public static void main(String[] args) {
    PiApp app = new PiApp();
    app.start();
  }

  private void start() {

    SparkSession spark = SparkSession.builder().appName("JavaSparkPi")
        .master("spark://10.0.100.81:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "2")
        .config("spark.driver.host", "10.0.100.182")
        .config("spark.executor.extraClassPath",
            "/home/jgp/net.jgp.labs.spark/target/labs-spark-2.2.0-jar-with-dependencies.jar")
        .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    int n = 1 * NUM_SAMPLES;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, NUM_SAMPLES);

    long t0 = System.currentTimeMillis();

    long count = dataSet.map(integer -> {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      return (x * x + y * y <= 1) ? 1 : 0;
    }).reduce((integer, integer2) -> integer + integer2);

    long t1 = System.currentTimeMillis();

    log.info("Pi is roughly ..... {}", 4.0 * count / n);
    log.info("Processing time ... {} ms", t1 - t0);

    spark.stop();
  }
}
