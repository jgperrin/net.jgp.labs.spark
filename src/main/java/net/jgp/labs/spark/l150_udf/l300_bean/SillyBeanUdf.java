package net.jgp.labs.spark.l150_udf.l300_bean;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;

public class SillyBeanUdf implements UDF1<Integer, Row> {
  private static final long serialVersionUID = -906376L;

  @Override
  public Row call(Integer x) {
    SillyBean sb = new SillyBean(x);
    // be careful, the oreder matters...
    return RowFactory.create(sb.getDate(), sb.getValue(), sb.getValueAsString());
  }
}
