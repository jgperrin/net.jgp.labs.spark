package net.jgp.labs.spark.x.udf;

import java.util.UUID;

import org.apache.spark.sql.api.java.UDF1;

public class UuidWithOneColumnSeedGenerator implements UDF1<String, String> {

  private static final long serialVersionUID = -455996424L;

  @Override
  public String call(String seed) throws Exception {
    return UUID.nameUUIDFromBytes(seed.getBytes()).toString();
  }

}
