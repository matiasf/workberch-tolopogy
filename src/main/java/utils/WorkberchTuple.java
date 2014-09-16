package main.java.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public class WorkberchTuple {

    private Map<String, Object> values = new HashMap<String, Object>();

    public WorkberchTuple(Tuple input) {
	List<String> inputFields = input.getFields().toList();
	for (String inputField : inputFields) {
	    values.put(inputField, input.getValueByField(inputField));
	}
    }

    public Map<String, Object> getValues() {
	return values;
    }

}
