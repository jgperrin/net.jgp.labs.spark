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
        SparkSession spark = SparkSession.builder()
                .appName("CSV on S3 to Dataset<Row>")
                .master("spark://10.0.100.81:7077")
                .config("spark.executor.memory", "1g")
                .config("spark.executor.cores", "1")
                .config("spark.cores.max", "2")
                .config("spark.driver.host", "10.0.100.182")
                .config("spark.executor.extraClassPath",
                        "/home/jgp/net.jgp.labs.spark/target/labs-spark-2.2.0-jar-with-dependencies.jar")
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key",
                "xxx");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key",
                "xxx");
        // spark.sparkContext().hadoopConfiguration().set("fs.s3n.endpoint",
        // "us-east-2");
        String bucket = "bucket_name";
        String key = "key";

        String filename = "s3a://" + bucket + "/" + key;

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
