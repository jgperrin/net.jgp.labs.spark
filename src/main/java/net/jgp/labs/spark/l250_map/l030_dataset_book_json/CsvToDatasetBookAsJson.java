package net.jgp.labs.spark.l250_map.l030_dataset_book_json;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.json.JSONObject;

import net.jgp.labs.spark.x.model.Book;
import net.jgp.labs.spark.x.model.BookJson;

public class CsvToDatasetBookAsJson implements Serializable {
	private static final long serialVersionUID = 4262746489728980066L;

	class BookMapper implements MapFunction<Row, String> {
		private static final long serialVersionUID = -8940709795225426457L;

		@Override
		public String call(Row value) throws Exception {
			Book b = new Book();
			b.setId(value.getAs("id"));
			b.setAuthorId(value.getAs("authorId"));
			b.setLink(value.getAs("link"));
			SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
			String stringAsDate = value.getAs("releaseDate");
			if (stringAsDate == null) {
				b.setReleaseDate(null);
			} else {
				b.setReleaseDate(parser.parse(stringAsDate));
			}
			b.setTitle(value.getAs("title"));

			BookJson bj = new BookJson();
			bj.setBook(b);
			JSONObject jo = bj.getBook();

			return jo.toString();
		}
	}

	public static void main(String[] args) {
		CsvToDatasetBookAsJson app = new CsvToDatasetBookAsJson();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset<Book> as JSON").master("local").getOrCreate();

		String filename = "data/books.csv";
		Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "true")
				.load(filename);
		df.show();

		Dataset<String> bookDf = df.map(new BookMapper(), Encoders.STRING());
		bookDf.show(20,132);

		Dataset<Row> bookAsJsonDf = spark.read().json(bookDf);
		bookAsJsonDf.show();
	}
}
