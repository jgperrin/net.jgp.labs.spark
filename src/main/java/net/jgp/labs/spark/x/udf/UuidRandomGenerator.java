package net.jgp.labs.spark.x.udf;

import java.util.UUID;

import org.apache.spark.sql.api.java.UDF0;

public class UuidRandomGenerator implements UDF0<String> {

  private static final long serialVersionUID = -45195996424L;

  @Override
  public String call() throws Exception {
    return UUID.randomUUID().toString();
  }

}
