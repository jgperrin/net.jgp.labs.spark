package net.jgp.labs.spark.l020_streaming.x.utils;

/**
 * Utility methods to help generate random records.
 * 
 * @author jgp
 */
public abstract class RecordGeneratorUtils {

	private static String[] fnames = { "John", "Kevin", "Lydia", "Nathan", "Jane", "Liz", "Sam", "Ruby", "Peter",
			"Mahendra", "Noah", "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Murthy", "Jonathan" };
	private static String[] lnames = { "Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main", "Haque",
			"Christie", "Khan", "Kahn", "Hahn" };

	public static String getRandomSSN() {
		return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-" + getRandomInt(10) + getRandomInt(10)
				+ "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + getRandomInt(10);
	}

	public static int getRandomInt(int i) {
		return (int) (Math.random() * i);
	}

	public static String getFirstName() {
		return fnames[getRandomInt(fnames.length)];
	}

	public static String getLastName() {
		return lnames[getRandomInt(lnames.length)];
	}

}
