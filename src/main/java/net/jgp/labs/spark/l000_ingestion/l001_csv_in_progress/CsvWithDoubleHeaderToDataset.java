package net.jgp.labs.spark.l000_ingestion.l001_csv_in_progress;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvWithDoubleHeaderToDataset {

	public static void main(String[] args) {
		System.out.println("Working directory = " + System.getProperty("user.dir"));
		CsvWithDoubleHeaderToDataset app = new CsvWithDoubleHeaderToDataset();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();

		String filename = "data/csv-double-header.txt";

		StructType schema = buildSchemaFromCsvDefinition("1st line of file", "2nd line of file"); // TODO

		// I use a dirty comment trick to avoid manipulating the data file, but
		// one could build the method...
		Dataset<Row> df = spark.read().schema(schema).option("inferSchema", "false").option("comment", "#").option("header", "true").option("mode", "DROPMALFORMED")
				.csv(filename);
		df.show();
		df.printSchema();

	}

	private StructType buildSchemaFromCsvDefinition(String colNames, String dataTypes) {
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("year", DataTypes.IntegerType, true),
						DataTypes.createStructField("length", DataTypes.DoubleType, true),
						DataTypes.createStructField("title", DataTypes.StringType, true),
						DataTypes.createStructField("subject", DataTypes.StringType, true),
						DataTypes.createStructField("actor", DataTypes.StringType, true),
						DataTypes.createStructField("actress", DataTypes.StringType, true),
						DataTypes.createStructField("director", DataTypes.StringType, true),
						DataTypes.createStructField("pop", DataTypes.DoubleType, true),
						DataTypes.createStructField("got Awards?", DataTypes.BooleanType, true),
						DataTypes.createStructField("** image **", DataTypes.StringType, true) });
		return schema;
	}
}
