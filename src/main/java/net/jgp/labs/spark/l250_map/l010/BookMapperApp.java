package net.jgp.labs.spark.l250_map.l010;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BookMapperApp implements Serializable {
	private static final long serialVersionUID = -5158472601494535920L;

	private final class BookPrinter implements MapFunction<Row, String> {
		private static final long serialVersionUID = 384463638437626547L;	

		@Override
		public String call(Row r) throws Exception {
			String s = r.getString(2) + " can be bought at " + r.getString(4);
			return s;
		}
	}

	public static void main(String[] args) {
		BookMapperApp app = new BookMapperApp();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("For Each Book").master("local").getOrCreate();

		String filename = "data/books.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "true")
				.load(filename);
		df.show();

		Dataset<String> ds = df.map(new BookPrinter(), Encoders.STRING());
		ds.printSchema();
		ds.show();
	}
}
