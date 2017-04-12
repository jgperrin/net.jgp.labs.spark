package net.jgp.labs.spark.x.datasource;

import org.apache.spark.sql.types.StructType;

import scala.Option;

/**
 * Represents an option StructType as required by a data source.
 * 
 * @author jgp
 */
public final class OptionStructType extends Option<StructType> {
	private static final long serialVersionUID = -5471711937223218817L;
	private StructType schema;

	public OptionStructType(StructType schema) {
		this.schema = schema;
	}

	@Override
	public boolean canEqual(Object arg0) {
		return false;
	}

	@Override
	public Object productElement(int arg0) {
		return null;
	}

	@Override
	public int productArity() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public StructType get() {
		return this.schema;
	}
}