package main.java.parser.model;

import java.util.List;

import main.java.spouts.InputNodeSpout;
import main.java.spouts.WorkberchGenericSpout;

public class WorkberchNodeInput implements WorkberchNode {

	List<String> outputs;
	DataGenerator dataGenerator;
	String name;
	
	public WorkberchNodeInput(final String name, final DataGenerator dataGenerator, final List<String> outputs) {
		this.name = name;
		this.dataGenerator = dataGenerator;
		this.outputs = outputs;
	}
	
	List<String> getOutput() {
		return outputs;
	}
	
	public DataGenerator getDataGenerator() {
		return dataGenerator;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public WorkberchGenericSpout buildSpout() {
		return new InputNodeSpout(outputs, dataGenerator);
	}

	
	
}
