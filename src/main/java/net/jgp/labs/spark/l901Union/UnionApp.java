package net.jgp.labs.spark.l901Union;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Use of unionByName() to create a complex header on a dataframe.
 * 
 * @author jgp
 */
public class UnionApp {
  private SimpleDateFormat format = null;

  public UnionApp() {
    this.format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   * @throws ParseException
   */
  public static void main(String[] args) throws ParseException {
    UnionApp app = new UnionApp();
    app.start();
  }

  /**
   * The processing code.
   * 
   * @throws ParseException
   */
  private void start() throws ParseException {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("expr()")
        .master("local")
        .getOrCreate();

    // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
    // HH:mm:ss", Locale.ENGLISH);

    // Data
    StructType dataSchema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "NAME",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "START_DATE",
            DataTypes.DateType,
            false),
        DataTypes.createStructField(
            "END_DATE",
            DataTypes.DateType,
            false),
        DataTypes.createStructField(
            "STATUS",
            DataTypes.StringType,
            false) });
    List<Row> dataRows = new ArrayList<Row>();
    dataRows.add(RowFactory.create("Alex", toDate("2018-01-01 00:00:00"),
        toDate("2018-02-01 00:00:00"), "OUT"));
    dataRows.add(RowFactory.create("Bob", toDate("2018-02-01 00:00:00"),
        toDate("2018-02-05 00:00:00"), "IN"));
    dataRows.add(RowFactory.create("Mark", toDate("2018-02-01 00:00:00"),
        toDate("2018-03-01 00:00:00"), "IN"));
    dataRows.add(RowFactory.create("Mark", toDate("2018-05-01 00:00:00"),
        toDate("2018-08-01 00:00:00"), "OUT"));
    dataRows.add(RowFactory.create("Meggy", toDate("2018-02-01 00:00:00"),
        toDate("2018-02-01 00:00:00"), "OUT"));
    Dataset<Row> dataDf = spark.createDataFrame(dataRows, dataSchema);
    dataDf.show();
    dataDf.printSchema();

    // Header
    StructType headerSchema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "_c1",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "_c2",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "_c3",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "_c4",
            DataTypes.StringType,
            false) });
    List<Row> headerRows = new ArrayList<Row>();
    headerRows.add(RowFactory.create("REQUEST_DATE",
        format.format(new java.util.Date()), "", ""));
    headerRows.add(RowFactory.create("USER", "Kate", "", ""));
    headerRows.add(RowFactory.create("SEARCH_TYPE", "Global", "", ""));
    headerRows.add(RowFactory.create("", "", "", ""));
    headerRows
        .add(RowFactory.create("NAME", "START_DATE", "END_DATE", "STATUS"));
    Dataset<Row> headerDf = spark.createDataFrame(headerRows, headerSchema);
    headerDf.show(false);
    headerDf.printSchema();

    // Transition
    Dataset<Row> transitionDf = dataDf
        .withColumn("_c1", dataDf.col("NAME"))
        .withColumn("_c2",
            dataDf.col("START_DATE").cast(DataTypes.StringType))
        .withColumn("_c3",
            dataDf.col("END_DATE").cast(DataTypes.StringType))
        .withColumn("_c4", dataDf.col("STATUS").cast(DataTypes.StringType))
        .drop("NAME")
        .drop("START_DATE")
        .drop("END_DATE")
        .drop("STATUS");
    transitionDf.show(false);
    transitionDf.printSchema();

    // Union
    Dataset<Row> unionDf = headerDf.unionByName(transitionDf);
    unionDf.show(false);
    unionDf.printSchema();
  }

  private Date toDate(String dateAsText) throws ParseException {
    java.util.Date parsed;
    parsed = format.parse(dateAsText);
    return new Date(parsed.getTime());
  }
}
