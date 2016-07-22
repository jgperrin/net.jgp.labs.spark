package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class HelloSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Hello Spark").setMaster("local");
		SparkContext sc = new SparkContext(conf); 
		System.out.println("Hello, Spark v." + sc.version());
	}

}
