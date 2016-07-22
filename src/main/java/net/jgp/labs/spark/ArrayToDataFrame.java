package net.jgp.labs.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.IntegerType;

public class ArrayToDataFrame {

	public static void main(String[] args) {
		ArrayToDataFrame app = new ArrayToDataFrame();
		app.start();
	}

	private void start() {
		SparkConf conf = new SparkConf().setAppName("Data Set from Array").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		Integer[] l = new Integer[] { 1, 2, 3, 4, 5, 6, 7 };
		List<Integer> data = Arrays.asList(l);
		System.out.println(data);
		DataFrame df = sqlContext.createDataFrame(data, IntegerType.class);
		
		// TODO this is display an empty array for now
		df.show();
	}
}