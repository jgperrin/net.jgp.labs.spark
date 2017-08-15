package net.jgp.labs.spark.l800_concurrency;

import org.apache.spark.sql.SparkSession;

/**
 * Work in progress, do not use.
 * 
 * @author jgp
 *
 */
public class StartMaster {

	public static void main(String[] args) {
		StartMaster.startAndWait(1000);
	}

	private static void startAndWait(int duration) {
		SparkSession spark = SparkSession.builder().appName("Server is starting").master("local").getOrCreate();

		for (int i = duration; i > 0; i--) {
			System.out.println("Will continue running for " + i + " s.");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Hmmm... Something interrupted the thread: " + e.getMessage());
			}
		}

	}

}
