package net.jgp.labs.spark.x.datasource;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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

public class SubStringCounterRelation extends BaseRelation implements Serializable, TableScan {

	private static final long serialVersionUID = -3441600156161255871L;
	private static Logger log = LoggerFactory.getLogger(SubStringCounterRelation.class);

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
			sfl.add(DataTypes.createStructField("line", DataTypes.IntegerType, true));
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
		FileReader fileReader;
		try {
			fileReader = new FileReader(filename);
		} catch (FileNotFoundException e) {
			log.error("File [{}] not found, got {}", filename, e.getMessage(), e);
			return emptyRow();
		}

		BufferedReader br = new BufferedReader(fileReader);
		String line;
		int lineCount = 0;
		int criteriaCount = this.criteria.size();
		List<List<Integer>> table = new ArrayList<>();
		List<Integer> row0;
		do {
			row0 = new ArrayList<>();
			try {
				line = br.readLine();
			} catch (IOException e) {
				log.error("Error while reading [{}], got {}", filename, e.getMessage(), e);
				break;
			}
			row0.add(lineCount);
			for (int i = 0; i < criteriaCount; i++) {
				int v = StringUtils.countMatches(line, this.criteria.get(i));
				row0.add(v);
			}
			// line.
			table.add(row0);
			lineCount++;
		} while (line != null);

		try {
			br.close();
		} catch (IOException e) {
			log.error("Error while closing [{}], got {}", filename, e.getMessage(), e);
		}

		// We will work now
		List<Integer> list = new ArrayList<>();
		list.add(45);

		@SuppressWarnings("resource") // cannot be closed here, done elsewhere
		JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());
		JavaRDD<Row> rowRDD = sparkContext.parallelize(table).map(row -> RowFactory.create(row.toArray()));

		return rowRDD.rdd();
	}

	private RDD<Row> emptyRow() {
		List<Integer> list = new ArrayList<>();
		@SuppressWarnings("resource") // cannot be closed here, done elsewhere
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
