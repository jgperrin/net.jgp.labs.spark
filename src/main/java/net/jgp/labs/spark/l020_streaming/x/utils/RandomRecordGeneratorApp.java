package net.jgp.labs.spark.l020_streaming.x.utils;

public class RandomRecordGeneratorApp {

	public static void main(String[] args) {
		RandomRecordGeneratorApp app = new RandomRecordGeneratorApp();
		app.start();
	}

	private void start() {
		StringBuilder record = new StringBuilder();
		int maxRecord = RecordGeneratorUtils.getRandomInt(10) + 1;
		for (int i = 0; i < maxRecord; i++) {
			String fname = RecordGeneratorUtils.getFirstName();
			record.append(fname);
			String lname = RecordGeneratorUtils.getLastName();
			record.append(',');
			record.append(lname);
			int age = RecordGeneratorUtils.getRandomInt(114) + 1;
			record.append(',');
			record.append(age);
			String ssn = RecordGeneratorUtils.getRandomSSN();
			record.append(',');
			record.append(ssn);
			record.append('\n');
		}
		RecordWriterUtils.write("contact_" + System.currentTimeMillis() + ".txt", record);
	}

}
