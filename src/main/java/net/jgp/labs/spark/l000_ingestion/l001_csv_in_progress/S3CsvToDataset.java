package net.jgp.labs.spark.l000_ingestion.l001_csv_in_progress;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class S3CsvToDataset {

    public static void main(String[] args) {
        S3CsvToDataset app = new S3CsvToDataset();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("CSV on S3 to Dataset<Row>")
                .master("local").getOrCreate();

        //spark.sparkContext().hadoopConfiguration().set("fs.s3n.endpoint", "us-east-2");
        String filename = "<bucket>/<key>";
      
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "false")
                .option("sep", "|")
                .load(filename);
        df.show();
        df.printSchema();
    }
}
