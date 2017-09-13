package net.jgp.labs.spark.x.utils.streaming;

import java.io.File;

public class StreamingUtils {

	private String inputDirectory;
	private static StreamingUtils instance = null;

	private static StreamingUtils getInstance() {
		if (instance == null) {
			instance = new StreamingUtils();
		}
		return instance;
	}

	private StreamingUtils() {
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			this.inputDirectory = "C:\\TEMP\\";
		} else {
			this.inputDirectory = System.getProperty("java.io.tmpdir");
		}
		this.inputDirectory += "streaming" + File.separator + "in" + File.separator;
	}

	public static boolean createInputDirectory() {
		File d = new File(getInputDirectory());
		return d.mkdirs();
	}

	public static String getInputDirectory() {
		return getInstance().inputDirectory;
	}

}
