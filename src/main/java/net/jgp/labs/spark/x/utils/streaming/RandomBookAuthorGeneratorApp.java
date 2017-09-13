package net.jgp.labs.spark.x.utils.streaming;

import net.jgp.labs.spark.x.app.record_generator.RecordGeneratorUtils;
import net.jgp.labs.spark.x.app.record_generator.RecordStructure;
import net.jgp.labs.spark.x.app.record_generator.RecordType;
import net.jgp.labs.spark.x.app.record_generator.RecordWriterUtils;

public class RandomBookAuthorGeneratorApp {

	public static void main(String[] args) {
		
		RecordStructure rsAuthor = new RecordStructure("author");
		rsAuthor.add("id", RecordType.ID);
		rsAuthor.add("fname", RecordType.FIRST_NAME);
		rsAuthor.add("lname", RecordType.LAST_NAME);
		rsAuthor.add("dob", RecordType.DATE_LIVING_PERSON, "MM/dd/yyyy");

		RecordStructure rsBook = new RecordStructure("book", rsAuthor);
		rsBook.add("id", RecordType.ID);
		rsBook.add("title", RecordType.TITLE);
		rsBook.add("authorId", RecordType.LINKED_ID);

		RandomBookAuthorGeneratorApp app = new RandomBookAuthorGeneratorApp();
		app.start(rsAuthor);
		app.start(rsBook);
	}

	private void start(RecordStructure rs) {
		int maxRecord = RecordGeneratorUtils.getRandomInt(10) + 1;
		RecordWriterUtils.write(rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
				rs.getRecords(maxRecord, true));
	}

}
