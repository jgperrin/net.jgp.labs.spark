package net.jgp.labs.spark.x.udf;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class VectorBuilder implements UDF1<Double, Vector> {
  private static final long serialVersionUID = -2991355883253063841L;

  @Override
  public Vector call(Double t1) throws Exception {
    return Vectors.dense(t1);
  }

}
