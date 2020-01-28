package net.jgp.labs.spark.x.utils;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;

public class DataframeUtils {

  public static Dataset<Row> addMetadata(Dataset<Row> df, String colName,
      String key, String value) {
    Metadata metadata = new MetadataBuilder()
        .withMetadata(ColumnUtils.getMetadata(df, colName))
        .putString(key, value)
        .build();
    Column col = col(colName);
    return df.withColumn(colName, col, metadata);
  }

  public static Dataset<Row> addMetadata(Dataset<Row> df, String key,
      String value) {
    for (String colName : df.columns()) {
      df = addMetadata(df, colName, key, value);
    }
    return df;
  }

}
