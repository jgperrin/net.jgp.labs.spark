package net.jgp.labs.spark.x.utils.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSessionSingleton {
  private static transient SparkSession instance = null;

  public static SparkSession getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = SparkSession.builder().config(sparkConf).getOrCreate();
    }
    return instance;
  }
}