package main.java.parser.model;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Values;

public interface DataGenerator extends Serializable {
	
	abstract public List<Values> getValues();
}
