package net.jgp.labs.spark.l999_scrapbook.l002;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Response to
 * https://stackoverflow.com/questions/69802146/convert-spark-df-to-a-ds-with-different-fields-names
 * 
 * @author jgp
 *
 */
public class DecoderApp {

  public static void main(String[] args) {
    DecoderApp app = new DecoderApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Playing with Pojo encoders")
        .master("local[*]")
        .getOrCreate();

    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "name",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "date_of_birth",
            DataTypes.DateType,
            false) });

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .schema(schema)
        .load("data/id_date.csv");

    // Step 1
    System.out.println("Step 1");
    df.show(false);
    df.printSchema();

    // Step 1
    System.out.println("Step 2");
    Encoder<Person> personEncoder = Encoders.bean(Person.class);

    Dataset<Row> exportDf = df
        .withColumn("dateOfBirth",
            col("date_of_birth").cast(DataTypes.StringType))
        .drop("date_of_birth");
    System.out.println("exportDf");
    exportDf.show(false);
    exportDf.printSchema();

    Dataset<Person> personDS = exportDf.as(personEncoder);
    System.out.println("personDS");
    personDS.show(false);
    personDS.printSchema();

    List<Person> personList = personDS.collectAsList();
    System.out.println("List from collect()");
    for (Person p : personList) {
      System.out.println(p.debug());
    }
  }
}
