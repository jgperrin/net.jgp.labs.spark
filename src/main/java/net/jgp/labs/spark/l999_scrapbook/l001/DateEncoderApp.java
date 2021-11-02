package net.jgp.labs.spark.l999_scrapbook.l001;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Date encode in POJO
 * 
 * @author jgp
 *
 */
public class DateEncoderApp {

  public static void main(String[] args) {
    DateEncoderApp app = new DateEncoderApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Playing with Pojo encoders")
        .master("local[*]")
        .getOrCreate();

    // Step 1
    System.out.println("Step 1");
    Dataset<Row> timeDf = buildTimeDf(spark);
    timeDf.show(false);
    timeDf.printSchema();
  }

  /**
   * Creates your initial dataframe
   * 
   * @param spark
   * @return
   */
  private Dataset<Row> buildTimeDf(SparkSession spark) {
    List<Person> data = new ArrayList<>();
    Person b = new Person(1, new Date());
    data.add(b);
    b = new Person(2, new Date());
    data.add(b);

    Dataset<Person> ds = spark.createDataset(
        data,
        Encoders.bean(Person.class));
    return ds.toDF();
  }
}
