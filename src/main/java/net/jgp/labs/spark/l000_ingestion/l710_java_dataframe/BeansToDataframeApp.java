package net.jgp.labs.spark.l000_ingestion.l710_java_dataframe;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BeansToDataframeApp {

 

  public static void main(String[] args) {
    BeansToDataframeApp app = new BeansToDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Beans to Dataframe")
        .master("local[*]")
        .getOrCreate();

    List<Bean> data = new ArrayList<>();
    Bean b = new Bean(5L, new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);
     b = new Bean(15L, new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);

    Dataset<Bean> ds = spark.createDataset(data, Encoders.bean(Bean.class));
    Dataset<Row> df = ds.toDF();
    df.show();
    df.printSchema();
  }
}

