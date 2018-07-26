package net.jgp.labs.spark.x.utils.streaming;

import java.io.File;

public class StreamingUtils {

  private String inputDirectory;
  private String inputSubDirectory1;
  private String inputSubDirectory2;
  private static StreamingUtils instance = null;

  private static StreamingUtils getInstance() {
    if (instance == null) {
      instance = new StreamingUtils();
    }
    return instance;
  }

  /**
   * @return the inputSubDirectory1
   */
  public static String getInputSubDirectory1() {
    return getInstance().inputSubDirectory1;
  }

  /**
   * @return the inputSubDirectory2
   */
  public static String getInputSubDirectory2() {
    return getInstance().inputSubDirectory2;
  }

  private StreamingUtils() {
    if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
      this.inputDirectory = "C:\\TEMP\\";
    } else {
      this.inputDirectory = System.getProperty("java.io.tmpdir");
    }
    this.inputDirectory += "streaming" + File.separator + "in" + File.separator;
    createInputDirectory(this.inputDirectory);
    this.inputSubDirectory1 += this.inputDirectory + File.separator + "s1"
        + File.separator;
    createInputDirectory(this.inputSubDirectory1);
    this.inputSubDirectory2 += this.inputDirectory + File.separator + "s2"
        + File.separator;
    createInputDirectory(this.inputSubDirectory2);
  }

  public static boolean createInputDirectory() {
    return createInputDirectory(getInputDirectory());
  }

  private static boolean createInputDirectory(String directory) {
    File d = new File(directory);
    return d.mkdirs();
  }

  public static String getInputDirectory() {
    return getInstance().inputDirectory;
  }

}
