package net.jgp.labs.spark.l150_udf.l300_bean;

import static org.apache.spark.sql.functions.call_udf;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import net.jgp.labs.spark.l999_scrapbook.l002.Person;

public class UdfReturnBeanApp implements Serializable {
  private static final long serialVersionUID = 01500200L;

  public static void main(String[] args) {
    UdfReturnBeanApp app = new UdfReturnBeanApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Another lab")
        .master("local[*]").getOrCreate();

    // registers a new internal UDF
    spark.udf().register("bean", new SillyBeanUdf(), Encoders.bean(SillyBean.class).schema());

    String filename = "data/tuple-data-file.csv";
    Dataset<Row> df =
        spark.read().format("csv")
            .option("inferSchema", true)
            .option("header", false)
            .load(filename);
    df = df.withColumn("label", df.col("_c0")).drop("_c0");
    df = df.withColumn("value", df.col("_c1")).drop("_c1");
    df = df.withColumn(
        "beanX",
        call_udf("bean", df.col("value")));
    df.show(false);
    df.printSchema();
  }
}
