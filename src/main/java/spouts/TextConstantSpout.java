package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class TextConstantSpout extends WorkberchGenericSpout{

	private String textConstant;
	
	private boolean emitedValue = false;
	

	private static final long serialVersionUID = 1328441086506865276L;
	
	public TextConstantSpout(List<String> fields, String textConstant) {
		super(fields);
		this.textConstant = textConstant;
		
	}

	@Override
	public void nextTuple() {
		if (emitedValue) {
			emitedValue = true;
			this.collector.emit(new Values(textConstant));
		}
		
	}
	
	public String getTextConstant() {
		return textConstant;
	}

}
