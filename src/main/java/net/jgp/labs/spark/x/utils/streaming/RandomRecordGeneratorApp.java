package net.jgp.labs.spark.x.utils.streaming;

import net.jgp.labs.spark.x.app.record_generator.RecordGeneratorUtils;
import net.jgp.labs.spark.x.app.record_generator.RecordStructure;
import net.jgp.labs.spark.x.app.record_generator.RecordType;
import net.jgp.labs.spark.x.app.record_generator.RecordWriterUtils;

public class RandomRecordGeneratorApp {

	public static void main(String[] args) {
		RecordStructure rs = new RecordStructure("contact");
		rs.add("fname", RecordType.FIRST_NAME);
		rs.add("lname", RecordType.LAST_NAME);
		rs.add("age", RecordType.AGE);
		rs.add("ssn", RecordType.SSN);

		RandomRecordGeneratorApp app = new RandomRecordGeneratorApp();
		app.start(rs);
	}

	private void start(RecordStructure rs) {
		int maxRecord = RecordGeneratorUtils.getRandomInt(10) + 1;
		RecordWriterUtils.write(rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
				rs.getRecords(maxRecord, true));
	}

}
