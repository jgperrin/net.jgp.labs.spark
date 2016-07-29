/**
 * Example on how to connect to a remote Spark server/cluster to see if it is live.
 */
package net.jgp.labs.spark;

import org.apache.spark.sql.SparkSession;

public class ConnectRemotely {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("myApp").master("spark://10.0.100.120:7077").getOrCreate();
		System.out.println("Hello, Spark v." + spark.version());
	}
}
