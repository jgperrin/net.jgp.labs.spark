package net.jgp.labs.spark.l300_reduce.l000;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ReducerApp implements Serializable {
  private static final long serialVersionUID = -8951059218545451099L;

  private final class SumByReduce implements ReduceFunction<Integer> {
    private static final long serialVersionUID = -7580381094052442862L;

    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
      return v1 + v2;
    }
  }

  public static void main(String[] args) {
    ReducerApp ra = new ReducerApp();
    ra.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();

    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    Dataset<Integer> df = spark.createDataset(data, Encoders.INT());
    df.show();
    df.printSchema();
    Integer sumByReduce = df.reduce(new SumByReduce());
    System.out.println("Sum should be 55 and it is... " + sumByReduce);
  }
}
