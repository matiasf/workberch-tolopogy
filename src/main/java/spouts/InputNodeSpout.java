package main.java.spouts;

import java.util.List;

import main.java.parser.model.DataGenerator;
import backtype.storm.tuple.Values;

public class InputNodeSpout extends WorkberchGenericSpout {

	private static final long serialVersionUID = 1L;
	
	DataGenerator dataGenerator;
	
	public InputNodeSpout(final List<String> fields, final DataGenerator dataGenerator) {
		super(fields);
		
		this.dataGenerator = dataGenerator;
	}

	@Override
	public List<Values> getValues() {
		return dataGenerator.getValues();
	}

	@Override
	public void emitNextTuple(final Values values) {
		collector.emit(values);	
		System.out.println(values);
		
	}

}
