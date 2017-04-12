package net.jgp.labs.spark.x.datasource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class CharCounterDataSource implements FileFormat {

	@Override
	public Function1<PartitionedFile, Iterator<InternalRow>> buildReader(SparkSession arg0, StructType arg1,
			StructType arg2, StructType arg3, Seq<Filter> arg4, Map<String, String> arg5, Configuration arg6) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Function1<PartitionedFile, Iterator<InternalRow>> buildReaderWithPartitionValues(SparkSession arg0,
			StructType arg1, StructType arg2, StructType arg3, Seq<Filter> arg4, Map<String, String> arg5,
			Configuration arg6) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Option<StructType> inferSchema(SparkSession arg0, Map<String, String> arg1, Seq<FileStatus> arg2) {
		// Build your schema
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("count", DataTypes.IntegerType, true) });
		return new OptionStructType(schema);
	}

	@Override
	public boolean isSplitable(SparkSession arg0, Map<String, String> arg1, Path arg2) {
		return false;
	}

	@Override
	public OutputWriterFactory prepareWrite(SparkSession arg0, Job arg1, Map<String, String> arg2, StructType arg3) {
		return null;
	}

	@Override
	public boolean supportBatch(SparkSession arg0, StructType arg1) {
		return false;
	}

}
