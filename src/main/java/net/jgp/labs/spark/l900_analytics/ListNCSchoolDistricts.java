/**
 * NC schools by school district analysis.
 */
package net.jgp.labs.spark.l900_analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.FileUtils;


/**
 * @author jgp
 *
 */
public class ListNCSchoolDistricts {

  /**
   * @param args
   */
  public static void main(String[] args) {
    String filename = "/tmp/" + System.currentTimeMillis() + ".json";
    try {
      FileUtils.copyURLToFile(
          new URL("https://opendurham.nc.gov/explore/dataset/north-carolina-school-performance-data/download/?format=json&timezone=America/New_York"),
          new File(filename));
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("File " + filename + " downloaded");

    SparkSession spark = SparkSession.builder().appName("NC Schools").master("local")
        .getOrCreate();

    String fileToAnalyze = "/tmp/" + filename;
    System.out.println("File to analyze: " + fileToAnalyze);

    Dataset<Row> df;
    df = spark.read().option("dateFormat", "yyyy-mm-dd").json(fileToAnalyze);
    df = df.withColumn("district", df.col("fields.district"));
    df = df.groupBy("district").count().orderBy(df.col("district"));
    df.show(150, false);
  }
}
