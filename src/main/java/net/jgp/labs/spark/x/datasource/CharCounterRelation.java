package net.jgp.labs.spark.x.datasource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CharCounterRelation extends BaseRelation implements Serializable, TableScan {

	private static final long serialVersionUID = -3441600156161255871L;
	private static Logger log = LoggerFactory.getLogger(CharCounterRelation.class);

	private SQLContext sqlContext;
	private String filename;

	@Override
	public StructType schema() {
		log.debug("-> schema()");
		StructType schema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("count", DataTypes.IntegerType, true) });
		return schema;
	}

	@Override
	public SQLContext sqlContext() {
		log.debug("-> sqlContext()");
		return this.sqlContext;
	}

	public void setSqlContext(SQLContext arg0) {
		this.sqlContext = arg0;
	}

	@Override
	public RDD<Row> buildScan() {
		log.debug("-> buildScan()");
		// TODO Auto-generated method stub
		List<Row> rows = null;
		rows = new ArrayList<>();

		List<Integer> list = new ArrayList<>();
		list.add(45);

		SparkContext sparkContext = this.sqlContext.sparkContext();
		// RowFactory.create(row);
		// JavaRDD<Row> rowRDD = sparkContext.parallelize(list).map((String row)
		// -> {
		// return RowFactory.create(row);
		// });

		return null;// rowRDD.rdd();
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

}
