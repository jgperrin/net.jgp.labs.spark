package net.jgp.labs.spark.x.datasource;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.labs.spark.l000_ingestion.CustomDataSourceToDataset;
import scala.collection.immutable.Map;

public class CharCounterDataSource2 implements RelationProvider {
	private static Logger log = LoggerFactory.getLogger(CustomDataSourceToDataset.class);

	@Override
	public BaseRelation createRelation(SQLContext arg0, Map<String, String> arg1) {
		log.debug("-> createRelation()");

		java.util.Map<String, String> javaMap = scala.collection.JavaConverters.mapAsJavaMapConverter(arg1).asJava();

		if (log.isDebugEnabled()) {
			for (java.util.Map.Entry<String, String> entry : javaMap.entrySet()) {
				log.debug(entry.getKey() + "/" + entry.getValue());
			}
		}

		CharCounterRelation br = new CharCounterRelation();
		br.setSqlContext(arg0);

		return br;
	}

}
