/**
 * Example on how to connect to a remote Spark server/cluster to see if it is live.
 */
package net.jgp.labs.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class ConnectRemotely {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("myApp").setMaster("spark://10.0.100.120:7077");
		SparkContext sc = new SparkContext(conf);
		System.out.println("Hello, Remote Spark v." + sc.version());
	}
}
