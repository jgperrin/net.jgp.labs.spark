package net.jgp.labs.spark.l150_udf.l200_uuid;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import net.jgp.labs.spark.x.udf.UuidRandomGenerator;

/**
 * Reads a CSV file and generates a random UUID for each record, using the
 * UDF built in <code>UuidRandomGenerator</code>, in the
 * <code>net.jgp.labs.spark.x.udf</code> package.
 * 
 * @author jgp
 *
 */
public class AddRandomUuidInDataframeApp {

  public static void main(String[] args) {
    AddRandomUuidInDataframeApp app = new AddRandomUuidInDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Random UUID in dataframe app")
        .master("local[*]")
        .getOrCreate();

    spark.udf().register(
        "uuid_random",
        new UuidRandomGenerator(),
        DataTypes.StringType);

    String filename = "data/authors.csv";
    Dataset<Row> df =
        spark.read()
            .format("csv")
            .option("inferSchema", true)
            .option("header", true)
            .load(filename);
    df = df.withColumn("uuid", callUDF("uuid_random"));
    df.show(false);

    spark.stop();
  }
}
