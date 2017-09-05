package net.jgp.labs.spark.x.udf;

import org.apache.spark.sql.api.java.UDF1;

public class Stringifier implements UDF1<Integer, String> {

	private static final long serialVersionUID = -4519338105113996424L;

	@Override
	public String call(Integer t1) throws Exception {
		return "v" + t1.toString();
	}
}
