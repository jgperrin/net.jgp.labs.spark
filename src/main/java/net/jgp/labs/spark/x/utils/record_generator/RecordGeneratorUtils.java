package net.jgp.labs.spark.x.utils.record_generator;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Utility methods to help generate random records.
 * 
 * @author jgp
 */
public abstract class RecordGeneratorUtils {

	private static String[] fnames = { "John", "Kevin", "Lydia", "Nathan", "Jane", "Liz", "Sam", "Ruby", "Peter",
			"Mahendra", "Noah", "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Murthy", "Jonathan", "Jean",
			"Georges", "Oliver" };
	private static String[] lnames = { "Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main", "Haque",
			"Christie", "Khan", "Kahn", "Hahn", "Sanders" };
	private static String[] articles = { "The", "My", "A", "Your", "Their" };
	private static String[] adjectives = { "", "Great", "Beautiful", "Better", "Worse", "Gorgeous", "Terrific",
			"Terrible", "Natural", "Wild" };
	private static String[] nouns = { "Life", "Trip", "Experience", "Work", "Job", "Beach" };
	private static int[] daysInMonth = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

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

	public static String getArticle() {
		return articles[getRandomInt(articles.length)];
	}

	public static String getAdjective() {
		return adjectives[getRandomInt(adjectives.length)];
	}

	public static String getNoun() {
		return nouns[getRandomInt(nouns.length)];
	}

	public static String getTitle() {
		return (getArticle() + " " + getAdjective()).trim() + " " + getNoun();
	}

	public static int getIdentifier(List<Integer> identifiers) {
		int i;
		do {
			i = getRandomInt(60000);
		} while (identifiers.contains(i));

		return i;
	}

	public static Integer getLinkedIdentifier(List<Integer> linkedIdentifiers) {
		if (linkedIdentifiers == null) {
			return -1;
		}
		if (linkedIdentifiers.isEmpty()) {
			return -2;
		}
		int i = getRandomInt(linkedIdentifiers.size());
		return linkedIdentifiers.get(i);
	}

	public static String getLivingPersonDateOfBirth(String format) {
		Calendar d = Calendar.getInstance();
		int year = d.get(Calendar.YEAR) - getRandomInt(120) + 15;
		int month = getRandomInt(12);
		int day = getRandomInt(daysInMonth[month]) + 1;
		d.set(year, month, day);

		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(d.getTime());
	}

}
