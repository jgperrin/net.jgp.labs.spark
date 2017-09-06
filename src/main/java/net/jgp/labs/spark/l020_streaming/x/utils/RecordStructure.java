package net.jgp.labs.spark.l020_streaming.x.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class RecordStructure {

	private String recordName;
	private LinkedHashMap<String, RecordType> columns;

	public RecordStructure(String recordName) {
		this.recordName = recordName;
		this.columns = new LinkedHashMap<>();
	}

	public void add(String columnName, RecordType recordType) {
		this.columns.put(columnName, recordType);
	}

	public StringBuilder getRecords(int recordCount, boolean header) {
		StringBuilder record = new StringBuilder();
		boolean first = true;
		if (header) {
			for (Map.Entry<String, RecordType> entry : this.columns.entrySet()) {
				if (first) {
					first = false;
				} else {
					record.append(',');
				}
				record.append(entry.getKey());
			}
			record.append('\n');
			first = true;
		}

		for (int i = 0; i < recordCount; i++) {
			for (Map.Entry<String, RecordType> entry : this.columns.entrySet()) {
				if (first) {
					first = false;
				} else {
					record.append(',');
				}
				switch (entry.getValue()) {
				case FIRST_NAME:
					record.append(RecordGeneratorUtils.getFirstName());
					break;
				case LAST_NAME:
					record.append(RecordGeneratorUtils.getLastName());
					break;
				case AGE:
					record.append(RecordGeneratorUtils.getRandomInt(114) + 1);
					break;
				case SSN:
					record.append(RecordGeneratorUtils.getRandomSSN());
					break;
				default:
					break;
				}
			}
			record.append('\n');
			first = true;
		}
		return record;
	}

	public String getRecordName() {
		return this.recordName;
	}

}
