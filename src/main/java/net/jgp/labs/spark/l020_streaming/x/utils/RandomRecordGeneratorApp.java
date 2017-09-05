package net.jgp.labs.spark.l020_streaming.x.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class RandomRecordGeneratorApp {

	String[] fnames = { "John", "Jane", "Liz", "Sam", "Ruby", "Peter" };
	String[] lnames = { "Smith", "Mills", "Perrin", "Foster" };

	public static void main(String[] args) {
		RandomRecordGeneratorApp app = new RandomRecordGeneratorApp();
		app.start();
	}

	private void start() {
		// Open file
		BufferedWriter out = null;
		try {
			FileWriter fstream = new FileWriter(
					StreamingUtils.getInputDirectory() + System.currentTimeMillis() + ".txt", true); // true tells to
																										// append data.
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		// Write
		int maxRecord = getRandomInt(10) + 1;
		for (int i = 0; i < maxRecord; i++) {
			String fname = fnames[getRandomInt(fnames.length)];
			String lname = lnames[getRandomInt(lnames.length)];
			int age = getRandomInt(114) + 1;
			String ssn = "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-" + getRandomInt(10)
					+ getRandomInt(10) + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)
					+ getRandomInt(10);
			String record = fname + "," + lname + "," + age + "," + ssn;
			try {
				out.write(record + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Close file
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private int getRandomInt(int i) {
		return (int) (Math.random() * i);
	}

}
