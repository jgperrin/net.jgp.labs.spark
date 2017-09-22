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

        String filename = "s3a://lumeris.data.platform.nonprod/PanamaCanal/dev/data/claim/EHIODS_0_1000_CLAIM_DTL";
        Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true")
                .option("header", "false")
                .load(filename);
        df.show();
        df.printSchema();
    }
}
