package net.jgp.labs.spark.x.datasource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
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
	private boolean updateSchema = true;

	/**
	 * Schema, never call directly, always rely on the schema() method.
	 */
	private StructType schema = null;
	private List<String> criteria = new ArrayList<>();

	/**
	 * Constructs the schema. Internally cache it to avoid rebuilding it every
	 * time.
	 */
	@Override
	public StructType schema() {
		log.debug("-> schema()");
		if (updateSchema || schema == null) {
			List<StructField> sfl = new ArrayList<>();
			for (String crit : this.criteria) {
				sfl.add(DataTypes.createStructField(crit, DataTypes.IntegerType, false));
			}
			schema = DataTypes.createStructType(sfl);
			updateSchema = false;
		}
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
		List<Integer> list = new ArrayList<>();
		list.add(45);

		JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());
		JavaRDD<Row> rowRDD = sparkContext.parallelize(list).map(row -> RowFactory.create(row));

		return rowRDD.rdd();
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public void addCriteria(String crit) {
		this.updateSchema = true;
		this.criteria.add(crit);
	}

}
