package net.jgp.labs.spark.l150_udf.l201_uuid_with_seed;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import net.jgp.labs.spark.x.udf.UuidWithOneColumnSeedGenerator;

/**
 * Reads a CSV file and generates a random UUID for each record, using the
 * UDF built in <code>UuidOneColumnGenerator</code>, in the
 * <code>net.jgp.labs.spark.x.udf</code> package.
 * 
 * You can observe that:
 * 
 * Author <code>Jean-Georges Perrin</code> with id 2 and id 16 have the same
 * UUID: <code>f42ad295-0377-36d8-833d-907af04760c4</code>.
 * 
 * Author <code>Jean Georges Perrin</code> with id 17 has a different UUID:
 * <code>dd476960-cc6f-3e89-a792-ae144056cbef</code>.
 * 
 * @author jgp
 */
public class AddSeededUuidInDataframeApp {

  public static void main(String[] args) {
    AddSeededUuidInDataframeApp app = new AddSeededUuidInDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Seeded UUID in dataframe app")
        .master("local[*]")
        .getOrCreate();

    spark.udf().register(
        "uuid1",
        new UuidWithOneColumnSeedGenerator(),
        DataTypes.StringType);

    String filename = "data/authors.csv";
    Dataset<Row> df =
        spark.read()
            .format("csv")
            .option("inferSchema", true)
            .option("header", true)
            .load(filename);
    df = df.withColumn("uuid", callUDF("uuid1", col("name")));
    df.show(false);

    spark.stop();
  }
}
