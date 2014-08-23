package main.java.utils;

import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WorkberchTuple {

    private List<Values> values;

    public List<Values> getValues() {
	return values;
    }

    public void setValues(List<Values> values) {
	this.values = values;
    }

}
