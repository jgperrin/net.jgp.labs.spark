package net.jgp.labs.spark.x.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ColumnUtils {

  public static String explain(Column col) {
    StringBuilder sb = new StringBuilder();

    sb.append("Name ....... ");
    sb.append(col.toString());

    return sb.toString();
  }

  public static Metadata getMetadata(Dataset<Row> df, String colName) {
    StructType schema = df.schema();
    StructField[] fields = schema.fields();
    for (StructField field : fields) {
      // TODO check on case
      if (field.name().compareTo(colName) == 0) {
        return field.metadata();
      }
    }
    return null;
  }

}
