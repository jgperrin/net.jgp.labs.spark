package net.jgp.labs.spark.l080_schema.l000_simple;

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
 * 
 * @author jgp
 *
 */
public class ComplexStructureApp {

  public static void main(String[] args) {
    ComplexStructureApp app = new ComplexStructureApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Array to Dataframe (Dataset<Row>)")
        .master("local")
        .getOrCreate();

    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "id",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "value-s",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "value-d",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "array",
            DataTypes.createArrayType(DataTypes.StringType, false),
            false),
        DataTypes.createStructField(
            "struct",
            DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                    "sid",
                    DataTypes.IntegerType,
                    false),
                DataTypes.createStructField(
                    "svalue",
                    DataTypes.StringType,
                    false) }),
            false),
        DataTypes.createStructField(
            "array-struct",
            DataTypes.createArrayType(
                DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField(
                        "asid",
                        DataTypes.IntegerType,
                        false),
                    DataTypes.createStructField(
                        "asvalue",
                        DataTypes.StringType,
                        false) })),
            false) });

    List<Row> rows = new ArrayList<>();
    for (int x = 0; x < 10; x++) {
      List<Row> subrows = new ArrayList<>();
      for (int y = 1000; y < 1003; y++) {
        subrows.add(RowFactory.create(y, "Sub " + y));
      }
      Row str = RowFactory.create(x * 5000, "Struct #" + x);
      String[] array =
          new String[] { "v" + (x * 100), "v" + (x * 100 + 1) };
      rows.add(
          RowFactory.create(x, "Value " + x, x / 4.0, array, str, subrows));
    }

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.show(false);
    df.printSchema();
  }
}
