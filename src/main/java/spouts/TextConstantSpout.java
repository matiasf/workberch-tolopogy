package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class TextConstantSpout extends WorkberchGenericSpout {

	private final String textConstant;
	
	private boolean emitedValue = false;
	

	private static final long serialVersionUID = 1328441086506865276L;
	
	public TextConstantSpout(final List<String> fields, final String textConstant) {
		super(fields);
		this.textConstant = textConstant;
		
	}
	
	public String getTextConstant() {
		return textConstant;
	}

	@Override
	public void emitNextTuple(final Values values) {
		if (emitedValue) {
			emitedValue = true;
			collector.emit(new Values(textConstant));
		}		
	}

}
