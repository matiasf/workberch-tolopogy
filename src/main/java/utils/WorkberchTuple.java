package main.java.utils;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public class WorkberchTuple {

	private final Map<String, Object> values = new HashMap<String, Object>();
	private final List<String> fields = new ArrayList<String>();

	public WorkberchTuple(final Tuple input) {
		final List<String> inputFields = input.getFields().toList();
		for (final String inputField : inputFields) {
			values.put(inputField, input.getValueByField(inputField));
		}
		fields.addAll(input.getFields().toList());
		fields.add(INDEX_FIELD);
	}

	public Map<String, Object> getValues() {
		return values;
	}
	
	public List<String> getFields() {
		return fields;
	}

}
