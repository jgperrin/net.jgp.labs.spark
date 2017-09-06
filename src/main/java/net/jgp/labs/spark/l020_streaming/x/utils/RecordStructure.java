package net.jgp.labs.spark.l020_streaming.x.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RecordStructure {

	private String recordName;
	private LinkedHashMap<String, RecordType> columns;
	private List<Integer> identifiers;
	private RecordStructure linkedRecord;

	public RecordStructure(String recordName) {
		init(recordName, null);
	}

	public RecordStructure(String recordName, RecordStructure linkedRecord) {
		init(recordName, linkedRecord);
	}

	private void init(String recordName, RecordStructure linkedRecord) {
		this.recordName = recordName;
		this.columns = new LinkedHashMap<>();
		this.identifiers = new ArrayList<>();
		this.linkedRecord = linkedRecord;
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
				case ID:
					int id = RecordGeneratorUtils.getIdentifier(this.identifiers);
					record.append(id);
					this.identifiers.add(id);
					break;
				case TITLE:
					record.append(RecordGeneratorUtils.getTitle());
					break;
				case LINKED_ID:
					if (this.linkedRecord == null) {
						record.append("null");
					} else {
						record.append(RecordGeneratorUtils.getLinkedIdentifier(this.linkedRecord.identifiers));
					}
					break;
				default:
					break;
				}
			}
			record.append('\n');
			first = true;
		}
		System.out.println(record);
		return record;
	}

	public String getRecordName() {
		return this.recordName;
	}

}
