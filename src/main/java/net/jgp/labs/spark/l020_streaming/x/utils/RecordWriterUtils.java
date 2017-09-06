package net.jgp.labs.spark.l020_streaming.x.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public abstract class RecordWriterUtils {

	public static void write(String filename, StringBuilder record) {
		String fullFilename = StreamingUtils.getInputDirectory() + filename;
		
		// Open file
		BufferedWriter out = null;
		try {
			FileWriter fstream = new FileWriter(fullFilename, true); // true
			// tells
			// to
			// append
			// data.
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.err.println("Error while opening file: " + e.getMessage());
		}

		// Write file
		try {
			out.write(record.toString());
		} catch (IOException e) {
			System.err.println("Error while writing: " + e.getMessage());
		}

		// Close file
		try {
			out.close();
		} catch (IOException e) {
			System.err.println("Error while closing file: " + e.getMessage());
		}

	}

}
