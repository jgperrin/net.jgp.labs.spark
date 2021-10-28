package net.jgp.labs.spark.l999_scrapbook.l000;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Response to
 * https://stackoverflow.com/questions/69738041/add-row-to-spark-dataframe-with-timestamps-and-id
 * 
 * @author jgp
 *
 */
public class AddRecordsToDataframeApp {

  public static void main(String[] args) {
    AddRecordsToDataframeApp app = new AddRecordsToDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Add record & id to an existing df")
        .master("local[*]")
        .getOrCreate();

    // Step 1
    // Read the df (here I am building it from scratch, but you can
    // use your existing code)
    System.out.println("Step 1");
    Dataset<Row> timeDf = buildTimeDf(spark);
    timeDf.show(false);
    timeDf.printSchema();

    // Step 2
    // Build the record to add: using a bean (similar code as the
    // buildTimeDf function)
    System.out.println("Step 2");
    List<ModelPrevisionRecord> data = new ArrayList<>();
    ModelPrevisionRecord b = new ModelPrevisionRecord(
        -1L,
        new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);
    b = new ModelPrevisionRecord(
        -1L,
        new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);
    Dataset<ModelPrevisionRecord> ds = spark.createDataset(data,
        Encoders.bean(ModelPrevisionRecord.class));
    timeDf = timeDf.unionByName(ds.toDF());
    timeDf.show(false);
    timeDf.printSchema();
    // At this point you will have:
    //@formatter:off
    //    +---+-----------------------+-----------------------+
    //    |id |model                  |prevision              |
    //    +---+-----------------------+-----------------------+
    //    |5  |2021-10-28 17:41:29.951|2021-10-28 17:41:29.951|
    //    |15 |2021-10-28 17:41:29.951|2021-10-28 17:41:29.951|
    //    |-1 |2021-10-28 17:41:32.681|2021-10-28 17:41:32.681|
    //    +---+-----------------------+-----------------------+
    //@formatter:on

    // Step 3
    // Replace bad id by good ones
    System.out.println("Step 3");
    timeDf = timeDf.withColumn("id2",
        when(
            col("id").$eq$eq$eq(-1), timeDf.agg(max("id")).head().getLong(0)+1)
                .otherwise(col("id")));
    timeDf.show(false);
    timeDf.printSchema();

    // Step 4
    // Cleanup
    System.out.println("Step 4");
    timeDf = timeDf.drop("id").withColumnRenamed("id2", "id");
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
    List<ModelPrevisionRecord> data = new ArrayList<>();
    ModelPrevisionRecord b = new ModelPrevisionRecord(5L,
        new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);
    b = new ModelPrevisionRecord(15L,
        new Timestamp(System.currentTimeMillis()),
        new Timestamp(System.currentTimeMillis()));
    data.add(b);

    Dataset<ModelPrevisionRecord> ds = spark.createDataset(data,
        Encoders.bean(ModelPrevisionRecord.class));
    return ds.toDF();
  }
}
