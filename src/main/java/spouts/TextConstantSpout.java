package main.java.spouts;

import java.util.List;

import main.java.parser.model.DataGenerator;
import main.java.parser.model.TextDataGenerator;
import backtype.storm.tuple.Values;

public class TextConstantSpout extends WorkberchGenericSpout {

	private final String textConstant;
	
	private final DataGenerator dataGenerator;
	

	private static final long serialVersionUID = 1328441086506865276L;
	
	public TextConstantSpout(final List<String> fields, final String textConstant) {
		super(fields);
		this.textConstant = textConstant;
		dataGenerator = new TextDataGenerator(textConstant);
	}
	
	public String getTextConstant() {
		return textConstant;
	}

	@Override
	public void emitNextTuple(final Values values) {
		collector.emit(values);
	}

	@Override
	public List<Values> getValues() {
		return dataGenerator.getValues();
	}

}
